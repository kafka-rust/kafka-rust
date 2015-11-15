extern crate kafka;
extern crate getopts;

use std::{env, fmt, process};
use std::fs::File;
use std::io::{self, Write, BufRead, BufReader};

use kafka::client::KafkaClient;

// how many brokers do we require to acknowledge the send messages
const REQUIRED_ACKS: i16 = 1;
// how long do we allow wainting for the acknowledgement
const ACK_TIMEOUT: i32 = 100;
// number of messages to send to kafka in one go/batch (only used for
// the file-based scenario)
const BATCH_SIZE: usize = 100;

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
    try!(client.load_metadata_all());

    // ~ verify that the remote brokers do know about the target topic
    if !client.topic_partitions.contains_key(&cfg.topic) {
        return Err(Error::Literal(format!("No such topic at {:?}: {}", cfg.brokers, cfg.topic)));
    }
    match cfg.input_file {
        None => produce_from_stdin(&mut client, &cfg.topic),
        Some(ref file) => produce_from_file(&mut client, &cfg.topic, &file),
    }
}

/// Read stdin and send every non-empty line one by one to kafka.
fn produce_from_stdin(client: &mut KafkaClient, topic: &str) -> Result<(), Error> {
    let mut line_buf = String::new();
    let stdin = io::stdin();
    let mut stderr = io::stderr();
    loop {
        line_buf.clear();
        if try!(stdin.read_line(&mut line_buf)) == 0 {
            break; // ~ EOF reached
        }
        let s = line_buf.trim();
        if s.is_empty() {
            continue; // ~ skip empty lines
        }
        // ~ now send the non-empty line to kafka
        let rs = try!(client.send_message(REQUIRED_ACKS, ACK_TIMEOUT, topic.to_owned(), s.as_bytes().into()));
        // ~ we can assert that there is exactly one returned response
        // for the sent message, since we already verified that the
        // underlying brokers do know about the target topic (see the
        // calling function) and the send message will end up in
        // exactly on partition.
        assert_eq!(1, rs.len());
        let _ = write!(stderr, "debug: {:?}\n", rs.get(0));
    }
    Ok(())
}

/// Read the specified file and send every non-empty line to kafka in
/// batches.
fn produce_from_file(client: &mut KafkaClient, topic: &str, filename: &str) -> Result<(), Error> {
    let mut r = BufReader::new(try!(File::open(filename)));
    let mut msg_buf = Vec::with_capacity(BATCH_SIZE);
    let mut line_buf = String::new();
    loop {
        line_buf.clear();
        if try!(r.read_line(&mut line_buf)) == 0 {
            break; // ~ EOF reached
        }
        let s = line_buf.trim();
        if s.is_empty() {
            continue; // ~ skip empty lines
        }
        // ~ buffer the line for later sending it as part of a bigger
        // batch
        msg_buf.push(kafka::utils::ProduceMessage {
            topic: topic.to_owned(),
            message: s.as_bytes().into(),
        });
        // ~ if we filled our batch send it out to kafka
        if msg_buf.len() >= BATCH_SIZE {
            try!(client.send_messages(REQUIRED_ACKS, ACK_TIMEOUT, msg_buf));
            msg_buf = Vec::with_capacity(BATCH_SIZE);
        }
    }
    // ~ flush pending messages
    if msg_buf.len() > 0 {
        try!(client.send_messages(REQUIRED_ACKS, ACK_TIMEOUT, msg_buf));
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
}

impl Config {
    fn from_cmdline() -> Result<Config, Error> {
        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "input", "Specify input file", "FILE");

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
        })
    }
}
