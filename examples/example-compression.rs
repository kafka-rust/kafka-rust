extern crate zstd;
extern crate kafka;
extern crate getopts;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use std::{env, process};
use std::fs::File;
use std::str::FromStr;
use std::io::{self, stdin, stderr, Write, BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

//use std::io::{Cursor, Read, Write};
use std::str::from_utf8;
use zstd::{Decoder, Encoder};
use kafka::compression::{self, zstandard};

use kafka::client::{KafkaClient, Compression, RequiredAcks, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS};
use kafka::producer::{AsBytes, Producer, Record, DEFAULT_ACK_TIMEOUT_MILLIS};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

fn main() {
    env_logger::init();

    let cfg_produce = match ConfigProducer::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    check_basic_compression();

    //test_produce_fetch_messages();
    match test_compression_producer(&cfg_produce) {
        Ok(_a) => println!("OK"),
        Err(_e) => println!("Error"),
    }

    let cfg_consume = match ConfigConsumer::from_cmdline() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    match test_compression_consume(cfg_consume) {
        Ok(_a) => println!("OK"),
        Err(_e) => println!("Error"),
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

fn check_basic_compression() {
    let b = "This string will be read".as_bytes();

    //let buffer = zstd::encode_all(&b[..], 3).unwrap();
    let buffer = zstandard::compress(b, 3).unwrap();

    //let new_buffer = zstd::decode_all(&buffer[..]).unwrap();

    let new_buffer = zstandard::uncompress(&buffer[..]).unwrap();

    let buf_str = from_utf8(&new_buffer).unwrap();

    println!("{}",buf_str);
}

fn test_compression_producer(cfg: &ConfigProducer) -> Result<()> {
    produce(cfg)
}


fn produce(cfg: &ConfigProducer) -> Result<()> {
    let mut client = KafkaClient::new(cfg.brokers.clone());
    client.set_client_id("kafka-rust-console-producer".into());
    client.load_metadata_all()?;

    // ~ verify that the remote brokers do know about the target topic
    if !client.topics().contains(&cfg.topic) {
        bail!(format!("No such topic at {:?}: {}", cfg.brokers, cfg.topic));
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

fn produce_impl(src: &mut BufRead, client: KafkaClient, cfg: &ConfigProducer) -> Result<()> {
    let mut producer = try!(
        Producer::from_client(client)
            .with_ack_timeout(cfg.ack_timeout)
            .with_required_acks(cfg.required_acks)
            .with_compression(cfg.compression)
            .with_connection_idle_timeout(cfg.conn_idle_timeout)
            .create()
    );
    if cfg.batch_size < 2 {
        produce_impl_nobatch(&mut producer, src, cfg)
    } else {
        produce_impl_inbatches(&mut producer, src, cfg)
    }
}

fn produce_impl_nobatch(producer: &mut Producer, src: &mut BufRead, cfg: &ConfigProducer) -> Result<()> {
    let mut stderr = stderr();
    //let mut rec = Record::from_value(&cfg.topic, Trimmed(String::new()));
    let mut rec = Record::from_value(&cfg.topic, Trimmed(String::from("Hello")));
    //loop {
        // rec.value.clear();
        // if try!(src.read_line(&mut rec.value)) == 0 {
        //     break; // ~ EOF reached
        // }
        // if rec.value.trim().is_empty() {
        //     continue; // ~ skip empty lines
        // }
        // ~ directly send to kafka
        producer.send(&rec)?;
        let _ = write!(stderr, "Sent: {}", *rec.value);
    //}
    Ok(())
}

// This implementation wants to be efficient.  It buffers N lines from
// the source and sends these in batches to Kafka.  Line buffers
// across batches are re-used for the sake of avoiding allocations.
fn produce_impl_inbatches(producer: &mut Producer, src: &mut BufRead, cfg: &ConfigProducer) -> Result<()> {
    assert!(cfg.batch_size > 1);

    // ~ a buffer of prepared records to be send in a batch to Kafka
    // ~ in the loop following, we'll only modify the 'value' of the
    // cached records
    let mut rec_stash: Vec<Record<(), Trimmed>> = (0..cfg.batch_size)
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

fn send_batch(producer: &mut Producer, batch: &[Record<(), Trimmed>]) -> Result<()> {
    let rs = try!(producer.send_all(batch));

    for r in rs {
        for tpc in r.partition_confirms {
            if let Err(code) = tpc.offset {
                bail!(ErrorKind::Kafka(kafka::error::ErrorKind::Kafka(code)));
            }
        }
    }

    Ok(())
}


// fn produce_impl(src: &mut BufRead, client: KafkaClient, cfg: &Config) -> Result<()> {
//     let mut producer = 
//         Producer::from_client(client)
//             .with_ack_timeout(cfg.ack_timeout)
//             .with_required_acks(cfg.required_acks)
//             .with_compression(cfg.compression)
//             .with_connection_idle_timeout(cfg.conn_idle_timeout)
//             .create()?;

//     //let mut rec = Record::from_value(&cfg.topic, Trimmed(String::new()));

//     let rec = Record::from_value(&cfg.topic, String::from("Hello, world!"));

//     if !rec.value.trim().is_empty() {
//         producer.send(&rec);

//         //println!("Sent: {}", *rec.value);
//     }

//     Ok(())
// }


fn test_compression_consume(cfg: ConfigConsumer) -> Result<()> {

    let mut cons_builder = Consumer::from_hosts(cfg.brokers)
            .with_group(cfg.group)
            .with_fallback_offset(cfg.fallback_offset)
            .with_fetch_max_wait_time(Duration::from_secs(1))
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .with_offset_storage(cfg.offset_storage)
            .with_client_id("kafka-rust-console-consumer".into());

    for topic in cfg.topics {
        cons_builder = cons_builder.with_topic(topic);
    }

    let mut c =  cons_builder.create().unwrap();
    
    let mut buf: Vec<u8> = Vec::with_capacity(1024);

    for ms in c.poll().unwrap().iter() {
        for m in ms.messages() {
            unsafe {buf.set_len(0)};

            let _ = write!(buf, "{}:{}@{}:\n", ms.topic(), ms.partition(), m.offset);
            //buf.extend_from_slice(m.value);
            
        }
        let _ = c.consume_messageset(ms);
    }

    c.commit_consumed();

    Ok(())
}

// --------------------------------------------------------------------

error_chain! {
    links {
        Kafka(kafka::error::Error, kafka::error::ErrorKind);
    }
    foreign_links {
        Io(io::Error);
        Opt(getopts::Fail);
    }
}

// --------------------------------------------------------------------

// struct Config {
//     brokers: Vec<String>,
//     topic: String,
//     group: String,
//     input_file: Option<String>,
//     compression: Compression,
//     offset_storage: GroupOffsetStorage,
//     required_acks: RequiredAcks,
//     batch_size: usize,
//     conn_idle_timeout: Duration,
//     ack_timeout: Duration,
//     fallback_offset: FetchOffset,

// }

// impl Config {
//     fn from_cmdline() -> Result<Config> {
//         use std::ascii::AsciiExt;

//         let args: Vec<String> = env::args().collect();
//         let mut opts = getopts::Options::new();
//         opts.optflag("h", "help", "Print this help screen");
//         opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
//         opts.optopt("", "topic", "Specify target topic", "NAME");
//         opts.optopt("", "group", "Specify target group", "GROUP");
//         opts.optopt("", "input", "Specify input file", "FILE");
//         opts.optopt("", "compression", "Compress messages [NONE, GZIP, SNAPPY]", "TYPE");
//         opts.optopt(
//             "",
//             "required-acks",
//             "Specify amount of required broker acknowledgments [NONE, ONE, ALL]",
//             "TYPE",
//         );
//         opts.optopt("", "ack-timeout", "Specify time to wait for acks", "MILLIS");
//         opts.optopt("", "batch-size", "Send N message in one batch.", "N");
//         opts.optopt("", "idle-timeout", "Specify timeout for idle connections", "MILLIS");

//         let m = match opts.parse(&args[1..]) {
//             Ok(m) => m,
//             Err(e) => bail!(e),
//         };
//         if m.opt_present("help") {
//             let brief = format!("{} [options]", args[0]);
//             bail!(opts.usage(&brief));
//         }
//         Ok(Config {
//             brokers: m.opt_str("brokers")
//                 .unwrap_or_else(|| "localhost:9092".to_owned())
//                 .split(',')
//                 .map(|s| s.trim().to_owned())
//                 .collect(),
//             topic: m.opt_str("topic").unwrap_or_else(|| "kafka-rust-test".to_owned()),
//             group: m.opt_str("group").unwrap_or_else(|| "my-group".to_owned()),
//             input_file: m.opt_str("input"),
//             compression: Compression::ZSTANDARD,
//             required_acks: match m.opt_str("required-acks") {
//                 None => RequiredAcks::One,
//                 Some(ref s) if s.eq_ignore_ascii_case("none") => RequiredAcks::None,
//                 Some(ref s) if s.eq_ignore_ascii_case("one") => RequiredAcks::One,
//                 Some(ref s) if s.eq_ignore_ascii_case("all") => RequiredAcks::All,
//                 Some(s) => bail!(format!("Unknown --required-acks argument: {}", s)),
//             },
//             batch_size: try!(to_number(m.opt_str("batch-size"), 1)),
//             conn_idle_timeout: Duration::from_millis(try!(to_number(
//                 m.opt_str("idle-timeout"),
//                 DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
//             ))),
//             ack_timeout: Duration::from_millis(
//                 try!(to_number(m.opt_str("ack-timeout"), DEFAULT_ACK_TIMEOUT_MILLIS)),
//             ),
//             offset_storage: GroupOffsetStorage::Zookeeper,
//             fallback_offset: FetchOffset::Earliest,
//         })
//     }
// }


// --------------------------------------------------------------------

struct ConfigProducer {
    brokers: Vec<String>,
    topic: String,
    input_file: Option<String>,
    compression: Compression,
    required_acks: RequiredAcks,
    batch_size: usize,
    conn_idle_timeout: Duration,
    ack_timeout: Duration,
}

impl ConfigProducer {
    fn from_cmdline() -> Result<ConfigProducer> {
        use std::ascii::AsciiExt;

        let args: Vec<String> = env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
        opts.optopt("", "topic", "Specify target topic", "NAME");
        opts.optopt("", "input", "Specify input file", "FILE");
        opts.optopt("", "compression", "Compress messages [NONE, GZIP, SNAPPY]", "TYPE");
        opts.optopt(
            "",
            "required-acks",
            "Specify amount of required broker acknowledgments [NONE, ONE, ALL]",
            "TYPE",
        );
        opts.optopt("", "ack-timeout", "Specify time to wait for acks", "MILLIS");
        opts.optopt("", "batch-size", "Send N message in one batch.", "N");
        opts.optopt("", "idle-timeout", "Specify timeout for idle connections", "MILLIS");

        let m = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => bail!(e),
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            bail!(opts.usage(&brief));
        }
        Ok(ConfigProducer {
            brokers: m.opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topic: m.opt_str("topic").unwrap_or_else(|| "kafka-rust-test".to_owned()),
            input_file: m.opt_str("input"),
            compression: match m.opt_str("compression") {
                None => Compression::NONE,
                Some(ref s) if s.eq_ignore_ascii_case("none") => Compression::NONE,
                #[cfg(feature = "gzip")]
                Some(ref s) if s.eq_ignore_ascii_case("gzip") => Compression::GZIP,
                #[cfg(feature = "snappy")]
                Some(ref s) if s.eq_ignore_ascii_case("snappy") => Compression::SNAPPY,
                #[cfg(feature = "zstandard")]
                Some(ref s) if s.eq_ignore_ascii_case("zstandard") => Compression::ZSTANDARD,
                Some(s) => bail!(format!("Unsupported compression type: {}", s)),
            },
            required_acks: match m.opt_str("required-acks") {
                None => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("none") => RequiredAcks::None,
                Some(ref s) if s.eq_ignore_ascii_case("one") => RequiredAcks::One,
                Some(ref s) if s.eq_ignore_ascii_case("all") => RequiredAcks::All,
                Some(s) => bail!(format!("Unknown --required-acks argument: {}", s)),
            },
            batch_size: try!(to_number(m.opt_str("batch-size"), 1)),
            conn_idle_timeout: Duration::from_millis(try!(to_number(
                m.opt_str("idle-timeout"),
                DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
            ))),
            ack_timeout: Duration::from_millis(
                try!(to_number(m.opt_str("ack-timeout"), DEFAULT_ACK_TIMEOUT_MILLIS)),
            ),
        })
    }
}

fn to_number<N: FromStr>(s: Option<String>, _default: N) -> Result<N> {
    match s {
        None => Ok(_default),
        Some(s) => {
            match s.parse::<N>() {
                Ok(n) => Ok(n),
                Err(_) => bail!(format!("Not a number: {}", s)),
            }
        }
    }
}



// --------------------------------------------------------------------

struct ConfigConsumer {
    brokers: Vec<String>,
    group: String,
    topics: Vec<String>,
    no_commit: bool,
    offset_storage: GroupOffsetStorage,
    fallback_offset: FetchOffset,
}

impl ConfigConsumer {
    fn from_cmdline() -> Result<ConfigConsumer> {
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
            Err(e) => { panic!(e.to_string()) },
        };
        if m.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            opts.usage(&brief);
        }

        macro_rules! required_list {
            ($name:expr) => {{
                let opt = $name;
                let xs: Vec<_> = match m.opt_str(opt) {
                    None => Vec::new(),
                    Some(s) => s.split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect(),
                };
                if xs.is_empty() {
                    format!("Invalid --{} specified!", opt);
                }
                xs
            }}
        };

        let mut brokers = required_list!("brokers");
        let mut topics = required_list!("topics");

        topics.push(String::from("kafka-rust-test"));
        brokers.push(String::from("localhost:9092"));

        let mut offset_storage = GroupOffsetStorage::Zookeeper;
        if let Some(s) = m.opt_str("storage") {
            if s.eq_ignore_ascii_case("zookeeper") {
                offset_storage = GroupOffsetStorage::Zookeeper;
            } else if s.eq_ignore_ascii_case("kafka") {
                offset_storage = GroupOffsetStorage::Kafka;
            } else {
               format!("unknown offset store: {}", s);
               ()
            }
        }
        Ok(ConfigConsumer {
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
