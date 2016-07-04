extern crate kafka;
extern crate getopts;
extern crate env_logger;
extern crate time;

use std::ascii::AsciiExt;
use std::env;
use std::fmt;
use std::io::{self, stdout, Write, BufWriter};
use std::process;
use std::thread;
use std::time as stdtime;

use kafka::client::{KafkaClient, FetchOffset, GroupOffsetStorage};

/// A very simple offset monitor for a particular topic able to show
/// the lag for a particular consumer group. Dumps the offset/lag of
/// the monitored topic/group to stdout every few seconds.
fn main() {
    env_logger::init().unwrap();

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("error: {}", e);
            process::exit(1);
        }
    };

    if let Err(e) = run(cfg) {
        println!("error: {}", e);
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

fn run(cfg: Config) -> Result<(), Error> {
    let mut client = KafkaClient::new(cfg.brokers);
    client.set_group_offset_storage(cfg.offset_storage);
    try!(client.load_metadata_all());

    let mut stdout = stdout();

    if cfg.topic.is_empty() {
        let ts = client.topics();
        let num_topics = ts.len();
        if num_topics == 0 {
            return Err(Error::Literal("no topics available".to_owned()));
        }
        let mut names: Vec<&str> = Vec::with_capacity(ts.len());
        names.extend(ts.names());
        names.sort();

        let mut buf = BufWriter::with_capacity(4096, stdout);
        for name in names {
            try!(write!(buf, "topic: {}\n", name));
        }
        return Err(Error::Literal("choose a topic".to_owned()));
    }

    let num_partitions = match client.topics().partitions(&cfg.topic) {
        None => return Err(Error::Literal(format!("no such topic: {}", &cfg.topic))),
        Some(partitions) => partitions.len(),
    };
    let mut offs = vec![Offsets::default(); num_partitions];
    let mut offs = offs.as_mut_slice();

    let mut fmt_buf = String::with_capacity(16);
    let mut out_buf = String::with_capacity(128);

    // ~ shall we produce the lag of the specified group?
    let show_lag = !cfg.group.is_empty();

    // ~ print a header
    {
        use std::fmt::Write;

        let _ = write!(out_buf, "{:>8} ", "time");
        for i in 0..num_partitions {
            fmt_buf.clear();
            let _ = write!(fmt_buf, "p-{}", i);
            let _ = write!(out_buf, " {:>10}", fmt_buf);
            if show_lag {
                let _ = write!(out_buf, " {:6}", "(lag)");
            }
        }
        if cfg.summary {
            let _ = write!(out_buf, " {:>12}", "summary");
            if show_lag {
                let _ = write!(out_buf, " {:8}", "(lag)");
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
        if show_lag {
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

            let mut total_latest = 0;
            let mut total_lag = 0;

            out_buf.clear();
            let _ = write!(out_buf, "{} ", t.strftime("%H:%M:%S").unwrap());
            for o in &*offs {
                total_latest += o.latest;
                let _ = write!(out_buf, " {:>10}", o.latest);
                if show_lag {
                    fmt_buf.clear();
                    if o.group < 0 {
                        let _ = write!(fmt_buf, "()");
                        total_lag = -1;
                    } else {
                        let mut lag = if cfg.commited_not_consumed {
                            o.latest - o.group
                        } else {
                            o.latest - o.group - 1
                        };
                        if lag < 0 {
                            // ~ it's quite likely that we fetch group offsets which
                            // are a bit ahead of the topic's latest offset since we
                            // issued the fetch-latest-offset request earlier than
                            // the request for the group-offsets
                            lag = 0;
                        }
                        if total_lag >= 0 {
                            total_lag += lag;
                        }
                        let _ = write!(fmt_buf, "({:<})", lag);
                    }
                    let _ = write!(out_buf, " {:6}", fmt_buf);
                }
            }
            if cfg.summary {
                let _ = write!(out_buf, " {:12}", total_latest);
                if show_lag {
                    fmt_buf.clear();
                    if total_lag >= 0 {
                        let _ = write!(fmt_buf, "({:<})", total_lag);
                    } else {
                        let _ = write!(fmt_buf, "()");
                    }
                    let _ = write!(out_buf, " {:8}", fmt_buf);
                }
            }
        }
        // ~ write the output
        out_buf.push('\n');
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
    commited_not_consumed: bool,
    summary: bool,
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
        opts.optflag("", "summary", "Print a summary for the topic");
        opts.optflag("", "committed-not-yet-consumed",
                     "Assume commited group offsets specify \
                      messages the group will start consuming \
                      (including those at these offsets)");

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
                .unwrap_or_else(|| String::new()),
            group: m.opt_str("group")
                .unwrap_or_else(|| String::new()),
            offset_storage: offset_storage,
            period: period,
            commited_not_consumed: m.opt_present("committed-not-yet-consumed"),
            summary: m.opt_present("summary"),
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
