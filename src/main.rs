use chrono::Duration;
use rand::seq::SliceRandom;
use std::collections::HashMap;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();

    let count = args
        .get(1)
        .and_then(|count| count.parse().ok())
        .unwrap_or(100_usize);

    let path = args
        .get(2)
        .cloned()
        .unwrap_or("/var/lib/vector/data/users.csv".to_string());

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(',' as u8)
        .from_path(path)
        .unwrap();

    let headers = reader
        .headers()
        .unwrap()
        .iter()
        .map(|col| col.to_string())
        .collect::<Vec<_>>();

    let data = reader
        .records()
        .map(|row| {
            let mut row = row
                .unwrap()
                .iter()
                .enumerate()
                .map(|(idx, col)| (headers[idx].as_str(), String::from(col)))
                .collect::<HashMap<&str, String>>();

            let dob =
                chrono::NaiveDate::parse_from_str(row.get("dob").unwrap(), "%Y-%m-%d").unwrap();
            row.insert(
                "from",
                (dob - Duration::days(7)).format("%Y-%m-%d").to_string(),
            );
            row.insert(
                "to",
                (dob + Duration::days(7)).format("%Y-%m-%d").to_string(),
            );

            row
        })
        .collect::<Vec<_>>();

    let mut rng = &mut rand::thread_rng();
    for _ in 0..count {
        let row = data.choose(&mut rng).unwrap();
        println!("{:?}", row);
    }
}
