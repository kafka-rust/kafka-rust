extern crate kafka;
extern crate getopts;

use std::{env, fmt, process};
use std::fs::File;
use std::io::{self, stdin, stderr, Write, BufRead, BufReader};
use std::ops::{Deref, DerefMut};

use kafka::client::{KafkaClient, Compression};
use kafka::producer::{AsBytes, Producer, Record};

// how many brokers do we require to acknowledge the send messages
const REQUIRED_ACKS: i16 = 1;
// how long do we allow wainting for the acknowledgement
const ACK_TIMEOUT: i32 = 100;

/// This is a very simple command line application sending every
/// non-empty line of standard input to a specified kafka topic; one
/// by one (not the most efficient way, of course).
///
/// Alternatively, messages can be read from an input file and sent do
/// kafka in batches (the typical use-case).
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
    let mut client = KafkaClient::new(cfg.brokers.clone());
    client.set_compression(cfg.compression);
    try!(client.load_metadata_all());

    // ~ verify that the remote brokers do know about the target topic
    if !client.topics().contains(&cfg.topic) {
        return Err(Error::Literal(format!("No such topic at {:?}: {}", cfg.brokers, cfg.topic)));
    }
    match cfg.input_file {
        None => {
            let stdin = stdin();
            let mut stdin = stdin.lock();
            produce_impl(&mut stdin, client, &cfg)
        }
        Some(ref file) => {
            let mut r = BufReader::new(try!(File::open(file)));
            produce_impl(&mut r, client, &cfg)
        }
    }
}

fn produce_impl(src: &mut BufRead, client: KafkaClient, cfg: &Config) -> Result<(), Error> {
    let mut producer = try!(Producer::from_client(client)
                            .with_ack_timeout(ACK_TIMEOUT)
                            .with_required_acks(REQUIRED_ACKS)
                            .create());
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

fn produce_impl_nobatch(producer: &mut Producer, src: &mut BufRead, cfg: &Config) -> Result<(), Error> {
    let mut stderr = stderr();
    let mut rec = Record::from_value(&cfg.topic, Trimmed(String::new()));
    loop {
        rec.value.clear();
        if try!(src.read_line(&mut rec.value)) == 0 {
            break; // ~ EOF reached
        }
        if rec.value.trim().is_empty() {
            continue; // ~ skip empty lines
        }
        // ~ directly send to kafka
        let r = try!(producer.send(&rec));
        let _ = write!(stderr, "debug: {:?}\n", r);
    }
    Ok(())
}

// This implementation wants to be efficient.  It buffers N lines from
// the source and sends these in batches to Kafka.  Line buffers
// across batches are re-used for the sake of avoiding allocations.
fn produce_impl_inbatches(producer: &mut Producer, src: &mut BufRead, cfg: &Config)
                          -> Result<(), Error>
{
    assert!(cfg.batch_size > 1);

    // ~ a buffer of prepared records to be send in a batch to Kafka
    // ~ in the loop following, we'll only modify the 'value' of the
    // cached records
    let mut rec_stash: Vec<Record<(), Trimmed>> =
        (0 .. cfg.batch_size)
        .map(|_| Record::from_value(&cfg.topic, Trimmed(String::new())))
        .collect();
    // ~ points to the next free slot in `rec_stash`.  if it reaches
    // `rec_stash.len()` we'll send `rec_stash` to kafka
    let mut next_rec = 0;
    loop {
        // ~ send out a batch if it's ready
        if next_rec == rec_stash.len() {
            try!(send_batch(producer, &rec_stash));
            next_rec = 0;
        }
        let mut rec = &mut rec_stash[next_rec];
        rec.value.clear();
        if try!(src.read_line(&mut rec.value)) == 0 {
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
        try!(send_batch(producer, &rec_stash[..next_rec]));
    }
    Ok(())
}

fn send_batch(producer: &mut Producer, batch: &[Record<(), Trimmed>]) -> Result<(), Error> {
    let rs = try!(producer.send_all(batch));
    for r in rs {
        if let Err(e) = r.offset {
            return Err(From::from(e));
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
    input_file: Option<String>,
    compression: Compression,
    batch_size: usize,
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "input", "Specify input file", "FILE");
        opts.optopt("", "compression", "Compress messages [NONE, GZIP, SNAPPY]", "TYPE");
        opts.optopt("", "batch-size", "Send N message in one batch.", "N");

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
            input_file: m.opt_str("input"),
            compression: {
                let s = m.opt_str("compression")
                    .unwrap_or_else(|| "NONE".to_owned());
                match s.trim() {
                    "none" | "NONE" => Compression::NONE,
                    "gzip" | "GZIP" => Compression::GZIP,
                    "snappy" | "SNAPPY" => Compression::SNAPPY,
                    _ => return Err(Error::Literal(format!("Unknown compression type: {}", s))),
                }
            },
            batch_size: {
                match m.opt_str("batch-size") {
                    None => 1,
                    Some(s) => {
                        match s.parse::<usize>() {
                            Ok(n) => n,
                            Err(_) => return Err(Error::Literal(format!("Not a number: {}", s))),
                        }
                    }
                }
            },
        })
    }
}
