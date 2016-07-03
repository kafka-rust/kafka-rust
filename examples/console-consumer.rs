extern crate kafka;
extern crate getopts;
extern crate env_logger;

use std::{env, io, fmt, process};
use std::ascii::AsciiExt;

use kafka::consumer::{Consumer, GroupOffsetStorage};

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
    if let Err(e) = process(cfg) {
        println!("{}", e);
        process::exit(1);
    }
}

fn process(cfg: Config) -> Result<(), Error> {
    let mut c =
        try!(Consumer::from_hosts(cfg.brokers, cfg.topic)
             .with_group(cfg.group)
             .with_fetch_max_wait_time(1000)
             .with_fetch_min_bytes(1_000)
             .with_fetch_max_bytes_per_partition(100_000)
             .with_retry_max_bytes_limit(1_000_000)
             .with_offset_storage(cfg.offset_storage)
             .create());

    let do_commit = !cfg.no_commit;
    loop {
        for ms in try!(c.poll()).iter() {
            for m in ms.messages() {
                let s = String::from_utf8_lossy(m.value);
                println!("{}:{}@{}: {}", ms.topic(), ms.partition(), m.offset, s.trim());
            }
            c.consume_messageset(ms);
        }
        if do_commit {
            try!(c.commit_consumed());
        }
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
    no_commit: bool,
    offset_storage: GroupOffsetStorage,
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<_> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "group", "Specify the consumer group", "NAME");
        opts.optflag("", "no-commit", "Do not commit consumed messages");
        opts.optopt("", "offsets", "Specify the offset store [zookeeper, kafka]", "STORE");

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(Error::Literal(e.to_string())),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            return Err(Error::Literal(opts.usage(&brief)));
        }

        let mut offset_storage = GroupOffsetStorage::Zookeeper;
        if let Some(s) = m.opt_str("offsets") {
            if s.eq_ignore_ascii_case("zookeeper") {
                offset_storage = GroupOffsetStorage::Zookeeper;
            } else if s.eq_ignore_ascii_case("kafka") {
                offset_storage = GroupOffsetStorage::Kafka;
            } else {
                return Err(Error::Literal(format!("unknown offset store: {}", s)));
            }
        }
        Ok(Config {
            brokers: m.opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            group: m.opt_str("group")
                .unwrap_or_else(|| String::new()),
            topic: m.opt_str("topic")
                .unwrap_or_else(|| "my-topic".to_owned()),
            no_commit: m.opt_present("no-commit"),
            offset_storage: offset_storage,
        })
   }
}
