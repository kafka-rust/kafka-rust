extern crate kafka;
extern crate getopts;
extern crate env_logger;

use std::{env, io, fmt, process};

use kafka::client::{KafkaClient, FetchOffset};
use kafka::consumer::Consumer;

/// This is a very simple command line application reading from a
/// specific kafka topic and dumping the messages to standard output.
fn main() {
    env_logger::init().unwrap();

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };
    if let Err(e) = process(&cfg) {
        println!("{}", e);
        process::exit(1);
    }
}

fn process(cfg: &Config) -> Result<(), Error> {
    // XXX too verbose to set up the consumer ... try avoiding the
    // indirection through KafkaClient
    let mut client = KafkaClient::new(cfg.brokers.clone());
    client.set_fetch_max_bytes_per_partition(1024*1024);
    try!(client.load_metadata_all());

    let mut c = Consumer::new(client, cfg.group.clone(), cfg.topic.clone())
        .with_fallback_offset(FetchOffset::Earliest);

    loop {
        for ms in try!(c.poll()).iter() {
            for m in ms.messages() {
                let s = String::from_utf8_lossy(m.value);
                println!("{}:{}@{}: {}", ms.topic(), ms.partition(), m.offset, s.trim());
            }
            c.consume_messageset(ms);
        }
        try!(c.commit_consumed());
    }
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
    group: String,
    topic: String,
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<_> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "group", "Specify the group_id file", "NAME");

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
            group: m.opt_str("group")
                .unwrap_or_else(|| "my-group".to_owned()),
            topic: m.opt_str("topic")
                .unwrap_or_else(|| "my-topic".to_owned()),
        })
   }
}
