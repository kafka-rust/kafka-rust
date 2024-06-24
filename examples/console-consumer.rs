use std::io::{self, Write};
use std::time::Duration;
use std::{env, process};
//use std::ascii::AsciiExt;

use anyhow::{bail, Result};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use tracing::{error, info};
use tracing_subscriber;

/// This is a very simple command line application reading from a
/// specific kafka topic and dumping the messages to standard output.
fn main() {
    tracing_subscriber::fmt::init();

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("{}", e);
            process::exit(1);
        }
    };
    info!(
        "Starting consumer with the following configuration: {:?}",
        cfg
    );

    if let Err(e) = process(cfg) {
        error!("{}", e);
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
        cb.create().unwrap()
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut buf = Vec::with_capacity(1024);

    loop {
        for ms in c.poll().unwrap().into_iter() {
            for m in ms.messages() {
                // ~ clear the output buffer
                unsafe { buf.set_len(0) };
                // ~ format the message for output
                let _ = writeln!(buf, "{}:{}@{}:", ms.topic(), ms.partition(), m.offset);
                buf.extend_from_slice(m.value);
                buf.push(b'\n');
                // ~ write to output channel
                stdout.write_all(&buf)?;
            }
            let _ = c.consume_messageset(&ms);
        }
        if cfg.commit {
            c.commit_consumed()?;
        }
        if !cfg.follow {
            return Ok(());
        }
    }
}

#[derive(Debug)]
struct Config {
    brokers: Vec<String>,
    group: String,
    topics: Vec<String>,
    commit: bool,
    offset_storage: Option<GroupOffsetStorage>,
    fallback_offset: FetchOffset,
    follow: bool,
}

impl Config {
    fn from_cmdline() -> Result<Config> {
        let args: Vec<_> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt(
            "",
            "brokers",
            "Specify kafka brokers (comma separated)",
            "HOSTS",
        );
        opts.optopt("", "topics", "Specify topics (comma separated)", "NAMES");
        opts.optopt("", "group", "Specify the consumer group", "NAME");

        opts.optflag("", "commit", "Commit group offsets");
        opts.optopt(
            "",
            "storage",
            "Specify the offset store [zookeeper, kafka]",
            "STORE",
        );

        opts.optflag(
            "",
            "earliest",
            "Fall back to the earliest offset (when no group offset available)",
        );
        opts.optflag("", "follow", "Continue reading from the topic indefinitely");

        macro_rules! on_error {
            ($name:expr) => {{
                let brief = format!("{} [options]", args[0]);
                println!("{}", opts.usage(&brief));
                bail!($name);
            }};
        }
        if args.len() == 1 {
            on_error!("no arguments provided");
        }

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => {
                on_error!(format!(
                    "argument parsing encountered an error: {}",
                    e.to_string()
                ))
            }
        };
        if m.opt_present("help") {
            on_error!("help requested");
        }

        macro_rules! required_list {
            ($name:expr) => {{
                let opt = $name;
                let xs: Vec<_> = match m.opt_str(opt) {
                    None => Vec::new(),
                    Some(s) => s
                        .split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect(),
                };
                if xs.is_empty() {
                    on_error!(format!("missing argument --{}", opt));
                }
                xs
            }};
        }

        let brokers = required_list!("brokers");
        let topics = required_list!("topics");

        let offset_storage = if let Some(s) = m.opt_str("storage") {
            if s.eq_ignore_ascii_case("zookeeper") {
                Some(GroupOffsetStorage::Zookeeper)
            } else if s.eq_ignore_ascii_case("kafka") {
                Some(GroupOffsetStorage::Kafka)
            } else {
                on_error!(format!("unknown offset store: {}", s));
            }
        } else {
            None
        };
        Ok(Config {
            brokers,
            group: m.opt_str("group").unwrap_or_default(),
            topics,
            commit: m.opt_present("commit"),
            offset_storage,
            fallback_offset: if m.opt_present("earliest") {
                FetchOffset::Earliest
            } else {
                FetchOffset::Latest
            },
            follow: m.opt_present("follow"),
        })
    }
}
