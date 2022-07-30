use anyhow::{ensure, Result};
use std::fs::File;
use std::io::{self, stderr, stdin, BufRead, BufReader, Write};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::time::Duration;
use std::{env, process};

use anyhow::anyhow;
use kafka::Error;

use kafka::client::{
    Compression, KafkaClient, RequiredAcks, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
};
use kafka::producer::{AsBytes, Producer, Record, DEFAULT_ACK_TIMEOUT_MILLIS};

/// This is a very simple command line application sending every
/// non-empty line of standard input to a specified kafka topic; one
/// by one (not the most efficient way, of course).
///
/// Alternatively, messages can be read from an input file and sent do
/// kafka in batches (the typical use-case).
fn main() {
    env_logger::init();

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

fn produce(cfg: &Config) -> Result<()> {
    let mut client = KafkaClient::new(cfg.brokers.clone());
    client.set_client_id("kafka-rust-console-producer".into());
    client.load_metadata_all()?;

    // ~ verify that the remote brokers do know about the target topic
    ensure!(client.topics().contains(&cfg.topic));
    match cfg.input_file {
        None => {
            let stdin = stdin();
            let mut stdin = stdin.lock();
            produce_impl(&mut stdin, client, cfg)
        }
        Some(ref file) => {
            let mut r = BufReader::new(File::open(file)?);
            produce_impl(&mut r, client, cfg)
        }
    }
}

fn produce_impl(src: &mut dyn BufRead, client: KafkaClient, cfg: &Config) -> Result<()> {
    let mut producer = Producer::from_client(client)
        .with_ack_timeout(cfg.ack_timeout)
        .with_required_acks(cfg.required_acks)
        .with_compression(cfg.compression)
        .with_connection_idle_timeout(cfg.conn_idle_timeout)
        .create()?;
    if cfg.batch_size < 2 {
        produce_impl_nobatch(&mut producer, src, cfg)
    } else {
        produce_impl_inbatches(&mut producer, src, cfg)
    }
}

struct Trimmed(String);

impl AsBytes for Trimmed {
    fn as_bytes(&self) -> &[u8] {
        self.0.trim().as_bytes()
    }
}

impl Deref for Trimmed {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Trimmed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn produce_impl_nobatch(
    producer: &mut Producer,
    src: &mut dyn BufRead,
    cfg: &Config,
) -> Result<()> {
    let mut stderr = stderr();
    let mut rec = Record::from_value(&cfg.topic, Trimmed(String::new()));
    loop {
        rec.value.clear();
        if src.read_line(&mut rec.value)? == 0 {
            break; // ~ EOF reached
        }
        if rec.value.trim().is_empty() {
            continue; // ~ skip empty lines
        }
        // ~ directly send to kafka
        producer.send(&rec)?;
        let _ = write!(stderr, "Sent: {}", *rec.value);
    }
    Ok(())
}

// This implementation wants to be efficient.  It buffers N lines from
// the source and sends these in batches to Kafka.  Line buffers
// across batches are re-used for the sake of avoiding allocations.
fn produce_impl_inbatches(
    producer: &mut Producer,
    src: &mut dyn BufRead,
    cfg: &Config,
) -> Result<()> {
    assert!(cfg.batch_size > 1);

    // ~ a buffer of prepared records to be send in a batch to Kafka
    // ~ in the loop following, we'll only modify the 'value' of the
    // cached records
    let mut rec_stash: Vec<Record<'_, (), Trimmed>> = (0..cfg.batch_size)
        .map(|_| Record::from_value(&cfg.topic, Trimmed(String::new())))
        .collect();
    // ~ points to the next free slot in `rec_stash`.  if it reaches
    // `rec_stash.len()` we'll send `rec_stash` to kafka
    let mut next_rec = 0;
    loop {
        // ~ send out a batch if it's ready
        if next_rec == rec_stash.len() {
            send_batch(producer, &rec_stash)?;
            next_rec = 0;
        }
        let rec = &mut rec_stash[next_rec];
        rec.value.clear();
        if src.read_line(&mut rec.value)? == 0 {
            break; // ~ EOF reached
        }
        if rec.value.trim().is_empty() {
            continue; // ~ skip empty lines
        }
        // ~ ok, we got a line. read the next one in a new buffer
        next_rec += 1;
    }
    // ~ flush pending messages - if any
    if next_rec > 0 {
        send_batch(producer, &rec_stash[..next_rec])?;
    }
    Ok(())
}

fn send_batch(producer: &mut Producer, batch: &[Record<'_, (), Trimmed>]) -> Result<()> {
    let rs = producer.send_all(batch)?;

    for r in rs {
        for tpc in r.partition_confirms {
            if let Err(code) = tpc.offset {
                //return Err(Error::Kafka(kafka::error::Error::Kafka(code)));
                return Err(anyhow!("{:?}", code));
            }
        }
    }

    Ok(())
}

struct Config {
    brokers: Vec<String>,
    topic: String,
    input_file: Option<String>,
    compression: Compression,
    required_acks: RequiredAcks,
    batch_size: usize,
    conn_idle_timeout: Duration,
    ack_timeout: Duration,
}

impl Config {
    fn from_cmdline() -> Result<Config> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt(
            "",
            "brokers",
            "Specify kafka brokers (comma separated)",
            "HOSTS",
        );
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "input", "Specify input file", "FILE");
        opts.optopt(
            "",
            "compression",
            "Compress messages [NONE, GZIP, SNAPPY]",
            "TYPE",
        );
        opts.optopt(
            "",
            "required-acks",
            "Specify amount of required broker acknowledgments [NONE, ONE, ALL]",
            "TYPE",
        );
        opts.optopt("", "ack-timeout", "Specify time to wait for acks", "MILLIS");
        opts.optopt("", "batch-size", "Send N message in one batch.", "N");
        opts.optopt(
            "",
            "idle-timeout",
            "Specify timeout for idle connections",
            "MILLIS",
        );

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(anyhow!("Error {:?}", e)),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            return Err(anyhow!("opts usage: {:?}", opts.usage(&brief)));
        }
        Ok(Config {
            brokers: m
                .opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topic: m.opt_str("topic").unwrap_or_else(|| "my-topic".to_owned()),
            input_file: m.opt_str("input"),
            compression: match m.opt_str("compression") {
                None => Compression::NONE,
                Some(ref s) if s.eq_ignore_ascii_case("none") => Compression::NONE,
                #[cfg(feature = "gzip")]
                Some(ref s) if s.eq_ignore_ascii_case("gzip") => Compression::GZIP,
                #[cfg(feature = "snappy")]
                Some(ref s) if s.eq_ignore_ascii_case("snappy") => Compression::SNAPPY,
                Some(s) => {
                    return Err(anyhow!(
                        "Error {:?}",
                        format!("Unsupported compression type: {}", s)
                    ))
                }
            },
            required_acks: match m.opt_str("required-acks") {
                None => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("none") => RequiredAcks::None,
                Some(ref s) if s.eq_ignore_ascii_case("one") => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("all") => RequiredAcks::All,
                Some(s) => {
                    return Err(anyhow!(
                        "{:?}",
                        format!("Unknown --required-acks argument: {}", s)
                    ))
                }
            },
            batch_size: to_number(m.opt_str("batch-size"), 1)?,
            conn_idle_timeout: Duration::from_millis(to_number(
                m.opt_str("idle-timeout"),
                DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
            )?),
            ack_timeout: Duration::from_millis(to_number(
                m.opt_str("ack-timeout"),
                DEFAULT_ACK_TIMEOUT_MILLIS,
            )?),
        })
    }
}

fn to_number<N: FromStr>(s: Option<String>, _default: N) -> Result<N> {
    match s {
        None => Ok(_default),
        Some(s) => match s.parse::<N>() {
            Ok(n) => Ok(n),
            Err(_) => return Err(anyhow!("{:?}", format!("Not a number: {}", s))),
        },
    }
}
