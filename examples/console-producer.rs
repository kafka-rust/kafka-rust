extern crate kafka;
extern crate getopts;

use std::{env, fmt, process};
use std::io::{self, Write};

/// This is a very simple command line application sending every
/// non-empty line of standard input to a specified kafka topic; one
/// by one (not the most efficient way, of course).
fn main() {
    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };
    if let Err(e) = produce(&cfg) {
        println!("{}", e);
        process::exit(1);
    }
}

fn produce(cfg: &Config) -> Result<(), Error> {
    let mut client = kafka::client::KafkaClient::new(cfg.brokers.clone());
    try!(client.load_metadata_all());

    // ~ verify that the remote brokers do know about the target topic
    if !client.topic_partitions.contains_key(&cfg.topic) {
        return Err(Error::Literal(format!("No such topic at {:?}: {}", cfg.brokers, cfg.topic)));
    }

    let mut line_buf = String::new();
    let stdin = io::stdin();
    let mut stderr = io::stderr();
    loop {
        line_buf.clear();
        match try!(stdin.read_line(&mut line_buf)) {
            0 => break, // EOF reached
            _ => {
                let s = line_buf.trim();
                if !s.is_empty() {
                    // ~ send the string to kafka
                    let r = try!(client.send_message(1, 100, cfg.topic.clone(), line_buf.as_bytes().into()));
                    // ~ we can assert that there is exactly one
                    // returned response for the sent message, since
                    // we already verified that the underlying brokers
                    // do know about the target topic
                    assert_eq!(1, r.len());
                    let _ = write!(stderr, "debug: {:?}\n", r.get(0));
                }
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------------

enum Error {
    Kafka(kafka::error::Error),
    Io(io::Error),
    Literal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::Kafka(ref e) => write!(f, "kafka-error: {}", e),
            &Error::Io(ref e) => write!(f, "io-error: {}", e),
            &Error::Literal(ref s) => write!(f, "{}", s),
        }
    }
}

impl From<kafka::error::Error> for Error {
    fn from(e: kafka::error::Error) -> Self { Error::Kafka(e) }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self { Error::Io(e) }
}

// --------------------------------------------------------------------

struct Config {
    brokers: Vec<String>,
    topic: String,
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify remove kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify the target topic", "NAME");

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(Error::Literal(e.to_string())),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            return Err(Error::Literal(opts.usage(&brief)));
        }
        Ok(Config {
            brokers: m.opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topic: m.opt_str("topic")
                .unwrap_or_else(|| "my-topic".to_owned()),
        })
    }
}
