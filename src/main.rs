mod generation;

use std::pin::pin;
use std::sync::Arc;

use async_stream::stream;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use tempfile::tempdir;
use tokio::fs::OpenOptions;
use vortex::IntoArray;
use vortex::arrays::StructArray;
use vortex::builders::ArrayBuilderExt;
use vortex::builders::builder_with_capacity;
use vortex::dtype::{DType, Nullability, PType, StructFields};
use vortex::error::vortex_err;
use vortex::file::VortexWriteOptions;
use vortex::stream::{ArrayStream, ArrayStreamAdapter};
use vortex::validity::Validity;
use vortex_datafusion::VortexFormat;

use generation::generate_logs;

const CHUNK_SIZE: usize = 100_000;
const RUNS_PER_QUERY: u32 = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;

    let filepath = temp_dir.path().join("a.vortex");

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&filepath)
        .await?;

    VortexWriteOptions::default()
        .write(f, row_array_stream(100_000_000))
        .await?;

    let ctx = SessionContext::new();
    let format = Arc::new(VortexFormat::default());
    let table_url = ListingTableUrl::parse(
        filepath
            .to_str()
            .ok_or_else(|| vortex_err!("Path is not valid UTF-8"))?,
    )?;
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(ListingOptions::new(format))
        .infer_schema(&ctx.state())
        .await?;

    let listing_table = Arc::new(ListingTable::try_new(config)?);

    ctx.register_table("vortex_tbl", listing_table as _)?;

    run_queries(&ctx).await
}

fn row_array_stream(num_rows: u64) -> impl ArrayStream + Unpin {
    // Define the DType for the BenchmarkLog struct
    let field_dtypes = vec![
        DType::Primitive(PType::U64, Nullability::NonNullable),
        DType::Utf8(Nullability::NonNullable),
        DType::Bool(Nullability::NonNullable),
        DType::Bool(Nullability::NonNullable),
        DType::Utf8(Nullability::NonNullable),
        DType::Primitive(PType::I32, Nullability::NonNullable),
        DType::Primitive(PType::U64, Nullability::NonNullable),
        DType::Utf8(Nullability::NonNullable),
    ];
    let benchmark_log_struct_fields: Arc<StructFields> = StructFields::new(
        vec![
            "id".into(),
            "message".into(),
            "message_matches_research".into(),
            "message_matches_team".into(),
            "country".into(),
            "severity".into(),
            "timestamp".into(),
            "metadata".into(),
        ]
        .into(),
        field_dtypes.clone(),
    )
    .into();
    let benchmark_log_dtype = DType::Struct(
        benchmark_log_struct_fields.clone(),
        Nullability::NonNullable,
    );

    // Create a stream that emits batches of documents as StructArrays.
    let stream = stream! {
        let mut chunks = pin!(generate_logs(num_rows).chunks(CHUNK_SIZE));
        while let Some(chunk) = chunks.next().await {
            let mut builders = field_dtypes.iter().map(|dtype| {
                builder_with_capacity(dtype.into(), CHUNK_SIZE)
            }).collect::<Vec<_>>();

            let chunk_len = chunk.len();
            for row in chunk {
                builders[0].append_scalar_value(row.id.into())?;
                builders[1].append_scalar_value(row.message.into())?;
                builders[2].append_scalar_value(row.message_matches_research.into())?;
                builders[3].append_scalar_value(row.message_matches_team.into())?;
                builders[4].append_scalar_value(row.country.into())?;
                builders[5].append_scalar_value(row.severity.into())?;
                builders[6].append_scalar_value(row.timestamp.into())?;
                builders[7].append_scalar_value(row.metadata.into())?;
            }

            yield Ok(StructArray::try_new_with_dtype(
                builders.into_iter().map(|mut b| b.finish()).collect(),
                benchmark_log_struct_fields.clone(),
                chunk_len,
                Validity::NonNullable,
            )?
            .into_array());
        }
    };

    ArrayStreamAdapter::new(benchmark_log_dtype, stream.boxed())
}

async fn run_queries(ctx: &SessionContext) -> anyhow::Result<()> {
    let queries = vec![
        (
            "count-filter",
            "SELECT COUNT(*) FROM vortex_tbl WHERE message_matches_team = true",
        ),
        ("count-nofilter", "SELECT COUNT(id) FROM vortex_tbl"),
        (
            "count-filter",
            "SELECT COUNT(id) FROM vortex_tbl WHERE message_matches_team = true",
        ),
        (
            "cardinality",
            "SELECT COUNT(DISTINCT severity) FROM vortex_tbl WHERE message_matches_research = true",
        ),
        (
            "bucket-string-nofilter",
            "SELECT country, COUNT(*) FROM vortex_tbl GROUP BY country ORDER BY country",
        ),
        (
            "bucket-string-filter",
            "SELECT country, COUNT(*) FROM vortex_tbl WHERE message_matches_research = true GROUP BY country ORDER BY country",
        ),
        (
            "bucket-numeric-nofilter",
            "SELECT severity, COUNT(*) FROM vortex_tbl GROUP BY severity ORDER BY severity",
        ),
        (
            "bucket-numeric-filter",
            "SELECT severity, COUNT(*) FROM vortex_tbl WHERE message_matches_research = true GROUP BY severity ORDER BY severity",
        ),
        (
            "top_n-compound",
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' ORDER BY severity, timestamp LIMIT 10",
        ),
        (
            "top_n-numeric-lowcard",
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' ORDER BY severity LIMIT 10",
        ),
        (
            "top_n-numeric-highcard",
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' ORDER BY timestamp LIMIT 10",
        ),
        (
            "top_n-string",
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' ORDER BY country LIMIT 10",
        ),
        (
            "filtered-lowcard",
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' AND severity < 3 LIMIT 10",
        ),
        (
            "filtered-highcard",
            // '2020-10-02' as a unix timestamp.
            "SELECT * FROM vortex_tbl WHERE message_matches_research = true AND country = 'Canada' AND timestamp >= 1601622000 LIMIT 10",
        ),
    ];
    for (query_type, query) in queries {
        run_query(ctx, query_type, query).await?;
    }

    Ok(())
}

async fn run_query(
    ctx: &SessionContext,
    query_type: impl AsRef<str>,
    query_string: impl AsRef<str>,
) -> anyhow::Result<()> {
    let query_type = query_type.as_ref();
    let query_string = query_string.as_ref();

    // ctx.sql(&format!("EXPLAIN {query_string}"))
    //     .await?
    //     .show()
    //     .await?;

    let start = std::time::Instant::now();
    let mut count = None;
    for _ in 0..RUNS_PER_QUERY {
        let current_count = ctx.sql(query_string).await?.collect().await?.len();
        if let Some(count) = count {
            assert_eq!(
                count, current_count,
                "Unstable result count for {query_string}."
            );
        } else {
            count = Some(current_count);
        }
    }
    println!(
        "{query_type}\t{} ms avg\t{} rows\t`{query_string}`",
        (start.elapsed() / RUNS_PER_QUERY).as_millis(),
        count.unwrap(),
    );

    Ok(())
}
