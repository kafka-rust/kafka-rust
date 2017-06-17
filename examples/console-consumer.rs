extern crate kafka;
extern crate getopts;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use std::{env, process};
use std::time::Duration;
use std::io::{self, Write};
use std::ascii::AsciiExt;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

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

fn process(cfg: Config) -> Result<()> {
    let mut c = {
        let mut cb = Consumer::from_hosts(cfg.brokers)
            .with_group(cfg.group)
            .with_fallback_offset(cfg.fallback_offset)
            .with_fetch_max_wait_time(Duration::from_secs(1))
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .with_offset_storage(cfg.offset_storage)
            .with_client_id("kafka-rust-console-consumer".into());
        for topic in cfg.topics {
            cb = cb.with_topic(topic);
        }
        try!(cb.create())
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut buf = Vec::with_capacity(1024);

    let do_commit = !cfg.no_commit;
    loop {
        for ms in try!(c.poll()).iter() {
            for m in ms.messages() {
                // ~ clear the output buffer
                unsafe { buf.set_len(0) };
                // ~ format the message for output
                let _ = write!(buf, "{}:{}@{}:\n", ms.topic(), ms.partition(), m.offset);
                buf.extend_from_slice(m.value);
                buf.push(b'\n');
                // ~ write to output channel
                try!(stdout.write_all(&buf));
            }
            let _ = c.consume_messageset(ms);
        }
        if do_commit {
            try!(c.commit_consumed());
        }
    }
}

// --------------------------------------------------------------------
error_chain! {
    foreign_links {
        Kafka(kafka::error::Error);
        Io(io::Error);
        Opt(getopts::Fail);
    }
}

// --------------------------------------------------------------------

struct Config {
    brokers: Vec<String>,
    group: String,
    topics: Vec<String>,
    no_commit: bool,
    offset_storage: GroupOffsetStorage,
    fallback_offset: FetchOffset,
}

impl Config {
    fn from_cmdline() -> Result<Config> {
        let args: Vec<_> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topics", "Specify topics (comma separated)", "NAMES");
        opts.optopt("", "group", "Specify the consumer group", "NAME");
        opts.optflag("", "no-commit", "Do not commit group offsets");
        opts.optopt("", "storage", "Specify the offset store [zookeeper, kafka]", "STORE");
        opts.optflag(
            "",
            "earliest",
            "Fall back to the earliest offset (when no group offset available)",
        );

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => bail!(e),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            bail!(opts.usage(&brief));
        }

        macro_rules! required_list {
            ($name:expr) => {{
                let opt = $name;
                let xs: Vec<_> = match m.opt_str(opt) {
                    None => bail!(format!("Required option --{} missing", opt)),
                    Some(s) => s.split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect(),
                };
                if xs.is_empty() {
                    bail!(format!("Invalid --{} specified!", opt));
                }
                xs
            }}
        };

        let brokers = required_list!("brokers");
        let topics = required_list!("topics");

        let mut offset_storage = GroupOffsetStorage::Zookeeper;
        if let Some(s) = m.opt_str("storage") {
            if s.eq_ignore_ascii_case("zookeeper") {
                offset_storage = GroupOffsetStorage::Zookeeper;
            } else if s.eq_ignore_ascii_case("kafka") {
                offset_storage = GroupOffsetStorage::Kafka;
            } else {
                bail!(format!("unknown offset store: {}", s));
            }
        }
        Ok(Config {
            brokers: brokers,
            group: m.opt_str("group").unwrap_or_else(|| String::new()),
            topics: topics,
            no_commit: m.opt_present("no-commit"),
            offset_storage: offset_storage,
            fallback_offset: if m.opt_present("earliest") {
                FetchOffset::Earliest
            } else {
                FetchOffset::Latest
            },
        })
    }
}
