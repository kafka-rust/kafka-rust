use std::cmp;
use std::collections::HashMap;
use std::env;
use std::io;
use std::process;

use kafka::client::{FetchOffset, KafkaClient};

/// Dumps available topic metadata to stdout.
fn main() {
    env_logger::init();

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };
    if let Err(e) = dump_metadata(cfg) {
        println!("{}", e);
        process::exit(1);
    }
}

struct Offsets {
    earliest: i64,
    latest: i64,
}

impl Default for Offsets {
    fn default() -> Self {
        Offsets {
            earliest: -1,
            latest: -1,
        }
    }
}

fn dump_metadata(cfg: Config) -> Result<(), String> {
    // ~ establish connection to kafka
    let mut client = KafkaClient::new(cfg.brokers);
    client.load_metadata_all().map_err(|e| e.to_string())?;
    // ~ determine the list of topics we're supposed to report about
    let topics = if cfg.topics.is_empty() {
        let topics = client.topics();
        let mut names = Vec::with_capacity(topics.len());
        for topic in topics.names() {
            names.push(topic.to_owned());
        }
        names.sort();
        names
    } else {
        cfg.topics
    };
    // ~ fetch the topics' earliest and latest offsets
    let (topic_width, offsets) = {
        let mut topic_width = 0;
        let mut m = HashMap::with_capacity(topics.len());
        let mut offsets = client
            .fetch_offsets(&topics, FetchOffset::Latest)
            .map_err(|e| e.to_string())?;
        for (topic, offsets) in offsets {
            topic_width = cmp::max(topic_width, topic.len());
            let mut offs = Vec::with_capacity(offsets.len());

            {
                // make sure to init the `offs` table based on the
                // clients metadata since that represents the set of
                // known partitions whereas the offsets we retrieved
                // can be missing some partitions if there are no
                // offsets for them, e.g. partition=0 might have no
                // offsets or might be out of order, while for
                // partition=1 we might have obtained an offset.  this
                // allows us later to use the partition as an index
                // into the table.
                let num_partitions = client
                    .topics()
                    .partitions(&topic)
                    .map(|ps| ps.len())
                    .unwrap_or(0);
                for _ in 0..num_partitions {
                    offs.push(Offsets::default());
                }
            }

            for offset in offsets {
                offs[offset.partition as usize].latest = offset.offset;
            }

            m.insert(topic, offs);
        }

        offsets = client
            .fetch_offsets(&topics, FetchOffset::Earliest)
            .map_err(|e| e.to_string())?;

        for (topic, offsets) in offsets {
            let offs = m.get_mut(&topic).expect("unknown topic");
            for offset in offsets {
                offs[offset.partition as usize].earliest = offset.offset;
            }
        }

        (topic_width + 2, m)
    };
    // ~ produce the output
    let mut out = io::stdout();
    let mut out_buf = String::with_capacity(128);
    let mut fmt_buf = if cfg.size {
        String::with_capacity(30)
    } else {
        String::new()
    };
    // ~ compute the width of the leader-hosts column
    let md = client.topics();
    let host_width = if cfg.host {
        2 + topics
            .iter()
            .filter_map(|t| md.partitions(t))
            .flatten()
            .map(|p| p.leader().map(|b| b.host().len()).unwrap_or(0))
            .fold(0, cmp::max)
    } else {
        0
    };
    // ~ print header line
    if cfg.header {
        use std::fmt::Write;
        let _ = write!(
            out_buf,
            "{1:0$} {2:4} {3:4}",
            topic_width, "topic", "p-id", "l-id"
        );
        if cfg.host {
            let _ = write!(out_buf, " {1:>0$}", host_width, "(l-host)");
        }
        let _ = write!(out_buf, " {:>12} {:>12}", "earliest", "latest");
        if cfg.size {
            let _ = write!(out_buf, " {:>12}", "(size)");
        }
        out_buf.push('\n');
        {
            use std::io::Write;
            let _ = out.write_all(out_buf.as_bytes());
        }
    }
    // ~ print each topic partition on its own line
    for (ti, topic) in topics.iter().enumerate() {
        match md.partitions(topic) {
            None => {
                use std::fmt::Write;
                out_buf.clear();
                if cfg.topic_separators && ti != 0 {
                    out_buf.push('\n');
                }
                let _ = writeln!(out_buf, "{1:0$} - not available!", topic_width, topic);
                {
                    use std::io::Write;
                    let _ = out.write_all(out_buf.as_bytes());
                }
            }
            Some(tmd) => {
                let offsets = &offsets[topic.as_str()];
                for (pi, p) in offsets.iter().enumerate() {
                    use std::fmt::Write;
                    out_buf.clear();
                    if cfg.topic_separators && ti != 0 && pi == 0 {
                        out_buf.push('\n');
                    }
                    let (leader_id, leader_host) = tmd
                        .partition(pi as i32)
                        .expect("unknown topic partition metadata")
                        .leader()
                        .map(|b| (b.id(), b.host()))
                        .unwrap_or((-1, ""));
                    let _ = write!(
                        out_buf,
                        "{1:0$} {2:>4} {3:>4}",
                        topic_width, topic, pi, leader_id
                    );
                    if cfg.host {
                        fmt_buf.clear();
                        let _ = write!(fmt_buf, "({})", leader_host);
                        let _ = write!(out_buf, " {1:0$}", host_width, fmt_buf);
                    }
                    let _ = write!(out_buf, " {:>12} {:>12}", p.earliest, p.latest);
                    if cfg.size {
                        fmt_buf.clear();
                        let _ = write!(fmt_buf, "({})", p.latest - p.earliest);
                        let _ = write!(out_buf, " {:>12}", fmt_buf);
                    }
                    out_buf.push('\n');
                    {
                        use std::io::Write;
                        let _ = out.write_all(out_buf.as_bytes());
                    }
                }
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------------

struct Config {
    brokers: Vec<String>,
    topics: Vec<String>,
    header: bool,
    host: bool,
    size: bool,
    topic_separators: bool,
}

impl Config {
    fn from_cmdline() -> Result<Config, String> {
        let args: Vec<_> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optflag("", "no-header", "Don't print headers");
        opts.optopt(
            "",
            "brokers",
            "Specify kafka brokers (comma separated)",
            "HOSTS",
        );
        opts.optopt("", "topics", "Specify topics (comma separated)", "NAMES");
        opts.optflag("", "no-host", "Don't print host:port of leaders");
        opts.optflag("", "no-size", "Don't print partition sizes");
        opts.optflag("", "no-empty-lines", "Don't separate topics by empty lines");
        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", &args[0]);
            return Err(opts.usage(&brief));
        }
        Ok(Config {
            brokers: m
                .opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topics: match m.opt_str("topics") {
                None => Vec::new(),
                Some(s) => s.split(',').map(|s| s.trim().to_owned()).collect(),
            },
            header: !m.opt_present("no-header"),
            host: !m.opt_present("no-host"),
            size: !m.opt_present("no-size"),
            topic_separators: !m.opt_present("no-empty-lines"),
        })
    }
}
