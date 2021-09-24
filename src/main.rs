use chrono::Duration;
use clap::{App, Arg};
use rand::seq::SliceRandom;
use std::{collections::HashMap, io::Write, net::TcpStream};

fn main() -> std::io::Result<()> {
    let matches = App::new("Enrich Data Gen")
        .version("0.3")
        .about("Does awesome things")
        .arg(
            Arg::with_name("path")
                .short("p")
                .long("path")
                .help("The path of the enrichment file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("count")
                .short("c")
                .long("count")
                .help("The number of records to sent")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .help("The host to send the data")
                .takes_value(true),
        )
        .get_matches();

    let path = matches
        .value_of("path")
        .unwrap_or("/var/lib/vector/data/users.csv");

    let count = matches
        .value_of("count")
        .and_then(|count| count.parse().ok());

    let host = matches.value_of("host").unwrap_or("127.0.0.1:9876");

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
    'forever: loop {
        println!("Connecting to {}", host);
        let mut stream = TcpStream::connect(host)?;
        match count {
            Some(count) => {
                for _ in 0..count {
                    let row = data.choose(&mut rng).unwrap();
                    let buf = format!("{:?}\n", row);
                    if let Err(_) = stream.write(buf.as_bytes()) {
                        // I realise an error here means the count starts from 0 again.
                        continue 'forever;
                    }
                }

                break 'forever;
            }
            None => loop {
                let row = data.choose(&mut rng).unwrap();
                let buf = format!("{:?}\n", row);
                if let Err(_) = stream.write(buf.as_bytes()) {
                    continue 'forever;
                }
            },
        }
    }

    Ok(())
}
