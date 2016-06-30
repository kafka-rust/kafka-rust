extern crate kafka;
extern crate getopts;
extern crate env_logger;
extern crate time;

use std::ascii::AsciiExt;
use std::env;
use std::fmt;
use std::io::{self, stdout, Write};
use std::process;
use std::thread;
use std::time as stdtime;

use kafka::client::{KafkaClient, FetchOffset, GroupOffsetStorage};

/// A very simple offset monitor for a particular topic able to show
/// the lag for a particular ocnsumer group. Dumps of the offset/lag
/// of the monitored topic/group to stdout.
fn main() {
    env_logger::init().unwrap();

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) =>{
            println!("{}", e);
            process::exit(1);
        }
    };

    if let Err(e) = monitor(cfg) {
        println!("{}", e);
        process::exit(1);
    }
}

#[derive(Copy, Clone)]
struct Offsets {
    latest: i64,
    group: i64,
}

impl Default for Offsets {
    fn default() -> Self {
        Offsets { latest: -1, group: -1 }
    }
}

fn monitor(cfg: Config) -> Result<(), Error> {
    let mut client = KafkaClient::new(cfg.brokers);
    client.set_group_offset_storage(cfg.offset_storage);
    try!(client.load_metadata_all());

    let num_partitions = match client.topics().partitions(&cfg.topic) {
        None => return Err(Error::Literal(format!("no such topic: {}", &cfg.topic))),
        Some(partitions) => partitions.len(),
    };
    let mut offs = vec![Offsets::default(); num_partitions];
    let mut offs = offs.as_mut_slice();

    let mut fmt_buf = String::with_capacity(16);
    let mut out_buf = String::with_capacity(128);

    let mut stdout = stdout();

    // ~ shall we produce the lag of the specified group?
    let lag = !cfg.group.is_empty();

    // ~ print a header
    {
        use std::fmt::Write;

        let _ = write!(out_buf, "{:>8}", "time");
        for i in 0..num_partitions {
            fmt_buf.clear();
            let _ = write!(fmt_buf, "p-{}", i);
            let _ = write!(out_buf, " {:>10}", fmt_buf);
            if lag {
                let _ = write!(out_buf, " {:6}", "(lag)");
            }
        }
        out_buf.push('\n');
        let _ = stdout.write_all(out_buf.as_bytes());
    }
    // ~ now periodically fetch the offsets and dump them to stdout
    loop {
        let t = time::now();
        // ~ latest topic offsets
        let latests = try!(client.fetch_topic_offsets(&cfg.topic, FetchOffset::Latest));
        for l in latests {
            let v = l.offset.unwrap_or(-1);
            offs.get_mut(l.partition as usize).expect("available partition").latest = v;
        }
        if lag {
            // ~ group offsets
            let groups = try!(client.fetch_group_topic_offsets(&cfg.group, &cfg.topic));
            for g in groups {
                let v = g.offset.unwrap_or(-1);
                offs.get_mut(g.partition as usize).expect("available partition").group = v;
            }
        }
        // ~ format the output
        {
            use std::fmt::Write;

            out_buf.clear();
            let _ = write!(out_buf, "{}", t.strftime("%H:%M:%S").unwrap());
            for o in &*offs {
                let _ = write!(out_buf, " {:>10}", o.latest);
                if lag {
                    fmt_buf.clear();
                    if o.group < 0 {
                        let _ = write!(fmt_buf, "()");
                    } else {
                        let _ = write!(fmt_buf, "({:<})", o.latest - o.group);
                    }
                    let _ = write!(out_buf, " {:6}", fmt_buf);
                }
            }
            out_buf.push('\n');
        }
        // ~ write the output
        let _ = stdout.write_all(out_buf.as_bytes());

        thread::sleep(cfg.period);
    }
}

// --------------------------------------------------------------------

struct Config {
    brokers: Vec<String>,
    topic: String,
    group: String,
    offset_storage: GroupOffsetStorage,
    period: stdtime::Duration,
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka bootstrap brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify the topic to monitor", "TOPIC");
        opts.optopt("", "group", "Specify the group to monitor", "GROUP");
        opts.optopt("", "offsets", "Specify offset store [zookeeper, kafka]", "STORE");
        opts.optopt("", "sleep", "Specify the sleep time", "SECS");

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
        let mut period = stdtime::Duration::from_secs(5);
        if let Some(s) = m.opt_str("sleep") {
            match s.parse::<u64>() {
                Ok(n) if n != 0 => period = stdtime::Duration::from_secs(n),
                _ => return Err(Error::Literal(format!("not a number greater than zero: {}", s))),
            }
        }
        Ok(Config {
            brokers: m.opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topic: m.opt_str("topic")
                .unwrap_or_else(|| "my-topic".to_owned()),
            group: m.opt_str("group")
                .unwrap_or_else(|| String::new()),
            offset_storage: offset_storage,
            period: period,
        })
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
