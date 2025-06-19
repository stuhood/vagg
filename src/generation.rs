/// Generates records in the same shape as:
/// https://github.com/paradedb/paradedb/blob/9598a6f6bc50a12cae344ead37b65dd10b450ed2/benchmarks/datasets/single/generate.sql
use async_stream::stream;
use futures::stream::Stream;
use serde::Serialize;

#[derive(Serialize)]
struct Metadata {
    value: i32,
    label: &'static str,
}

#[derive(Debug)]
pub struct BenchmarkLog {
    pub id: u64,
    pub message: &'static str,
    pub country: &'static str,
    pub severity: i32,
    pub timestamp: u64,
    pub metadata: String,
}

const MESSAGES: &[&str] = &[
    "The research team discovered a new species of deep-sea creature while conducting experiments near hydrothermal vents in the dark ocean depths.",
    "The research facility analyzed samples from ancient artifacts, revealing breakthrough findings about civilizations lost to the depths of time.",
    "The research station monitored weather patterns across mountain peaks, collecting data about atmospheric changes in the remote depths below.",
    "The research observatory captured images of stellar phenomena, peering into the cosmic depths to understand the mysteries of distant galaxies.",
    "The research laboratory processed vast amounts of genetic data, exploring the molecular depths of DNA to unlock biological secrets.",
    "The research center studied rare organisms found in ocean depths, documenting new species thriving in extreme underwater environments.",
    "The research institute developed quantum systems to probe subatomic depths, advancing our understanding of fundamental particle physics.",
    "The research expedition explored underwater depths near volcanic vents, discovering unique ecosystems adapted to extreme conditions.",
    "The research facility conducted experiments in the depths of space, testing how different materials behave in zero gravity environments.",
    "The research team engineered crops that could grow in the depths of drought conditions, helping communities facing climate challenges.",
];

const COUNTRIES: &[&str] = &[
    "United States",
    "Canada",
    "United Kingdom",
    "France",
    "Germany",
    "Japan",
    "Australia",
    "Brazil",
    "India",
    "China",
];

const LABELS: &[&str] = &[
    "critical system alert",
    "routine maintenance",
    "security notification",
    "performance metric",
    "user activity",
    "system status",
    "network event",
    "application log",
    "database operation",
    "authentication event",
];

pub fn generate_logs(num_rows: u64) -> impl Stream<Item = BenchmarkLog> {
    stream! {
        // '2020-01-01'
        let base_timestamp: u64 = 1577836800;

        for i in 0..(num_rows as usize) {
            let message = MESSAGES[i % MESSAGES.len()];
            let country = COUNTRIES[i % COUNTRIES.len()];
            let label = LABELS[i % LABELS.len()];
            let severity = (i % 5) as i32 + 1;
            let metadata_value = (i % 1000) as i32 + 1;

            // Timestamp calculation: '2020-01-01' + interval of days
            let days_to_add = (i % 731) as u64;
            let timestamp = base_timestamp + (86400 * days_to_add);

            yield BenchmarkLog {
                id: i as u64,
                message,
                country,
                severity,
                timestamp,
                metadata: serde_json::to_string(&Metadata {
                    value: metadata_value,
                    label,
                }).unwrap(),
            };
        }
    }
}
