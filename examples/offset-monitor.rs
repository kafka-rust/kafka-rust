#[macro_use]
use std::cmp;
use std::env;
use std::io::{self, stderr, stdout, BufWriter, Write};
use std::process;
use std::thread;
//use std::time as stdtime;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::{anyhow, Error, Result};
use kafka::client::{FetchOffset, GroupOffsetStorage, KafkaClient};

/// A very simple offset monitor for a particular topic able to show
/// the lag for a particular consumer group. Dumps the offset/lag of
/// the monitored topic/group to stdout every few seconds.
fn main() {
    env_logger::init();

    macro_rules! abort {
        ($e:expr) => {{
            let mut out = stderr();
            let _ = write!(out, "error: {}\n", $e);
            let _ = out.flush();
            process::exit(1);
        }};
    }

    let cfg = match Config::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => abort!(e),
    };

    if let Err(e) = run(cfg) {
        abort!(e);
    }
}

fn run(cfg: Config) -> Result<()> {
    let mut client = KafkaClient::new(cfg.brokers.clone());
    client.set_group_offset_storage(cfg.offset_storage);
    client.load_metadata_all()?;

    // ~ if no topic specified, print all available and be done.
    if cfg.topic.is_empty() {
        let ts = client.topics();
        let num_topics = ts.len();
        if num_topics == 0 {
            return Err(Error::from(anyhow!("no topics available")));
        }
        let mut names: Vec<&str> = Vec::with_capacity(ts.len());
        names.extend(ts.names());
        names.sort_unstable();

        let mut buf = BufWriter::with_capacity(1024, stdout());
        for name in names {
            let _ = writeln!(buf, "topic: {}", name);
        }
        return Err(Error::from(anyhow!("choose a topic")));
    }

    // ~ otherwise let's loop over the topic partition offsets
    let num_partitions = match client.topics().partitions(&cfg.topic) {
        None => return Err(Error::from(anyhow!("no such topic: {}", &cfg.topic))),
        Some(partitions) => partitions.len(),
    };
    let mut state = State::new(num_partitions, cfg.committed_not_consumed);
    let mut printer = Printer::new(stdout(), &cfg);
    printer.print_head(num_partitions)?;

    // ~ initialize the state
    let mut first_time = true;
    loop {
        let t = SystemTime::now();
        state.update_partitions(&mut client, &cfg.topic, &cfg.group)?;
        if first_time {
            state.curr_to_prev();
            first_time = false;
        }
        printer.print_offsets(&t, &state.offsets)?;
        thread::sleep(cfg.period);
    }
}

#[derive(Copy, Clone)]
struct Partition {
    prev_latest: i64,
    curr_latest: i64,
    curr_lag: i64,
}

impl Default for Partition {
    fn default() -> Self {
        Partition {
            prev_latest: -1,
            curr_latest: -1,
            curr_lag: -1,
        }
    }
}

struct State {
    offsets: Vec<Partition>,
    lag_decr: i64,
}

impl State {
    fn new(num_partitions: usize, committed_not_consumed: bool) -> State {
        State {
            offsets: vec![Default::default(); num_partitions],
            lag_decr: if committed_not_consumed { 0 } else { 1 },
        }
    }

    fn update_partitions(
        &mut self,
        client: &mut KafkaClient,
        topic: &str,
        group: &str,
    ) -> Result<()> {
        // ~ get the latest topic offsets
        let latests = client.fetch_topic_offsets(topic, FetchOffset::Latest)?;

        for l in latests {
            let off = self
                .offsets
                .get_mut(l.partition as usize)
                .expect("[topic offset] non-existent partition");
            off.prev_latest = off.curr_latest;
            off.curr_latest = l.offset;
        }

        if !group.is_empty() {
            // ~ get the current group offsets
            let groups = client.fetch_group_topic_offsets(group, topic)?;
            for g in groups {
                let off = self
                    .offsets
                    .get_mut(g.partition as usize)
                    .expect("[group offset] non-existent partition");

                // ~ it's quite likely that we fetched group offsets
                // which are a bit ahead of the topic's latest offset
                // since we issued the fetch-latest-offset request
                // earlier than the request for the group offsets
                off.curr_lag = cmp::max(0, off.curr_latest - g.offset - self.lag_decr);
            }
        }
        Ok(())
    }

    fn curr_to_prev(&mut self) {
        for o in &mut self.offsets {
            o.prev_latest = o.curr_latest;
        }
    }
}

struct Printer<W> {
    out: W,

    timefmt: String,
    fmt_buf: String,
    out_buf: String,

    time_width: usize,
    offset_width: usize,
    diff_width: usize,
    lag_width: usize,

    print_diff: bool,
    print_lag: bool,
    print_summary: bool,
}

impl<W: Write> Printer<W> {
    fn new(out: W, cfg: &Config) -> Printer<W> {
        Printer {
            out,
            timefmt: "%H:%M:%S".into(),
            fmt_buf: String::with_capacity(30),
            out_buf: String::with_capacity(160),
            time_width: 10,
            offset_width: 11,
            diff_width: 8,
            lag_width: 6,
            print_diff: cfg.diff,
            print_lag: !cfg.group.is_empty(),
            print_summary: cfg.summary,
        }
    }

    fn print_head(&mut self, num_partitions: usize) -> Result<()> {
        self.out_buf.clear();
        {
            // ~ format
            use std::fmt::Write;
            let _ = write!(self.out_buf, "{1:<0$}", self.time_width, "time");
            if self.print_summary {
                let _ = write!(self.out_buf, " {1:>0$}", self.offset_width, "topic");
                if self.print_diff {
                    let _ = write!(self.out_buf, " [{1:>0$}]", self.diff_width, "growth");
                }
                if self.print_lag {
                    let _ = write!(self.out_buf, " {1:0$}", self.lag_width, "(lag)");
                }
            } else {
                for i in 0..num_partitions {
                    self.fmt_buf.clear();
                    let _ = write!(self.fmt_buf, "p-{}", i);
                    let _ = write!(self.out_buf, " {1:>0$}", self.offset_width, self.fmt_buf);
                    if self.print_diff {
                        let _ = write!(self.out_buf, " [{1:>0$}]", self.diff_width, "growth");
                    }
                    if self.print_lag {
                        let _ = write!(self.out_buf, " {1:0$}", self.lag_width, "(lag)");
                    }
                }
            }
            self.out_buf.push('\n');
        }
        {
            // ~ print
            self.out.write_all(self.out_buf.as_bytes())?;
            Ok(())
        }
    }

    fn print_offsets(&mut self, time: &SystemTime, partitions: &[Partition]) -> Result<()> {
        self.out_buf.clear();
        {
            // ~ format
            use std::fmt::Write;

            self.fmt_buf.clear();
            let _ = write!(self.fmt_buf, "{}", time.elapsed().unwrap().as_secs());
            let _ = write!(self.out_buf, "{1:<0$}", self.time_width, self.fmt_buf);
            if self.print_summary {
                let mut prev_latest = 0;
                let mut curr_latest = 0;
                let mut curr_lag = 0;
                for p in partitions {
                    macro_rules! cond_add {
                        ($v:ident) => {
                            if $v != -1 {
                                if p.$v < 0 {
                                    $v = -1;
                                } else {
                                    $v += p.$v;
                                }
                            }
                        };
                    }

                    cond_add!(prev_latest);
                    cond_add!(curr_latest);
                    cond_add!(curr_lag);
                }
                let _ = write!(self.out_buf, " {1:>0$}", self.offset_width, curr_latest);
                if self.print_diff {
                    self.fmt_buf.clear();
                    let _ = write!(self.fmt_buf, "{:+}", curr_latest - prev_latest);
                    let _ = write!(self.out_buf, " [{1:>0$}]", self.diff_width, self.fmt_buf);
                }
                if self.print_lag {
                    self.fmt_buf.clear();
                    let _ = write!(self.fmt_buf, "({})", curr_lag);
                    let _ = write!(self.out_buf, " {1:<0$}", self.lag_width, self.fmt_buf);
                }
            } else {
                for p in partitions {
                    let _ = write!(self.out_buf, " {1:>0$}", self.offset_width, p.curr_latest);
                    if self.print_diff {
                        self.fmt_buf.clear();
                        let _ = write!(self.fmt_buf, "{:+}", p.curr_latest - p.prev_latest);
                        let _ = write!(self.out_buf, " [{1:>0$}]", self.diff_width, self.fmt_buf);
                    }
                    if self.print_lag {
                        self.fmt_buf.clear();
                        let _ = write!(self.fmt_buf, "({})", p.curr_lag);
                        let _ = write!(self.out_buf, " {1:<0$}", self.lag_width, self.fmt_buf);
                    }
                }
            }
        }
        self.out_buf.push('\n');
        self.out.write_all(self.out_buf.as_bytes())?;
        Ok(())
    }
}

// --------------------------------------------------------------------

struct Config {
    brokers: Vec<String>,
    topic: String,
    group: String,
    offset_storage: GroupOffsetStorage,
    period: Duration,
    committed_not_consumed: bool,
    summary: bool,
    diff: bool,
}

impl Config {
    fn from_cmdline() -> Result<Config> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt(
            "",
            "brokers",
            "Specify kafka bootstrap brokers (comma separated)",
            "HOSTS",
        );
        opts.optopt("", "topic", "Specify the topic to monitor", "TOPIC");
        opts.optopt("", "group", "Specify the group to monitor", "GROUP");
        opts.optopt(
            "",
            "storage",
            "Specify offset store [zookeeper, kafka]",
            "STORE",
        );
        opts.optopt("", "sleep", "Specify the sleep time", "SECS");
        opts.optflag(
            "",
            "partitions",
            "Print each partition instead of the summary",
        );
        opts.optflag("", "no-growth", "Don't print offset growth");
        opts.optflag(
            "",
            "committed-not-yet-consumed",
            "Assume committed group offsets specify \
                      messages the group will start consuming \
                      (including those at these offsets)",
        );

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(Error::from(e)),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            return Err(Error::from(anyhow!(opts.usage(&brief))));
        }
        let mut offset_storage = GroupOffsetStorage::Zookeeper;
        if let Some(s) = m.opt_str("storage") {
            if s.eq_ignore_ascii_case("zookeeper") {
                offset_storage = GroupOffsetStorage::Zookeeper;
            } else if s.eq_ignore_ascii_case("kafka") {
                offset_storage = GroupOffsetStorage::Kafka;
            } else {
                return Err(Error::from(anyhow!("unknown offset store: {}", s)));
            }
        }
        let mut period = Duration::from_secs(5);
        if let Some(s) = m.opt_str("sleep") {
            match s.parse::<u64>() {
                Ok(n) if n != 0 => period = Duration::from_secs(n),
                _ => {
                    return Err(Error::from(anyhow!(
                        "not a number greater than zero: {}",
                        s
                    )))
                }
            }
        }
        Ok(Config {
            brokers: m
                .opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topic: m.opt_str("topic").unwrap_or_else(String::new),
            group: m.opt_str("group").unwrap_or_else(String::new),
            offset_storage,
            period,
            committed_not_consumed: m.opt_present("committed-not-yet-consumed"),
            summary: !m.opt_present("partitions"),
            diff: !m.opt_present("no-growth"),
        })
    }
}
