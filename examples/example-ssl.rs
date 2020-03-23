#[macro_use]
extern crate log;

fn main() {
    example::main();
}

#[cfg(feature = "security")]
mod example {
    extern crate kafka;
    extern crate rustls;

    use std::env;
    use std::process;

    use self::kafka::client::{FetchOffset, KafkaClient};

    pub fn main() {
        env_logger::init().unwrap();

        // ~ parse the command line arguments
        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
        };

        // ~ instantiate KafkaClient with the default TLS configuration setup
        let mut client = KafkaClient::new_secure(cfg.brokers, rustls::ClientConfig::new());

        // ~ communicate with the brokers
        match client.load_metadata_all() {
            Err(e) => {
                println!("{:?}", e);
                drop(client);
                process::exit(1);
            }
            Ok(_) => {
                // ~ at this point we have successfully loaded
                // metadata via a secured connection to one of the
                // specified brokers

                if client.topics().len() == 0 {
                    println!("No topics available!");
                } else {
                    // ~ now let's communicate with all the brokers in
                    // the cluster our topics are spread over

                    let topics: Vec<String> = client.topics().names().map(Into::into).collect();
                    match client.fetch_offsets(topics.as_slice(), FetchOffset::Latest) {
                        Err(e) => {
                            println!("{:?}", e);
                            drop(client);
                            process::exit(1);
                        }
                        Ok(toffsets) => {
                            println!("Topic offsets:");
                            for (topic, mut offs) in toffsets {
                                offs.sort_by_key(|x| x.partition);
                                println!("{}", topic);
                                for off in offs {
                                    println!("\t{}: {:?}", off.partition, off.offset);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    struct Config {
        brokers: Vec<String>,
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt(
                "",
                "brokers",
                "Specify kafka brokers (comma separated)",
                "HOSTS",
            );

            let args: Vec<_> = env::args().collect();
            let m = match opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(e) => return Err(format!("{}", e)),
            };

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            };

            let brokers = m
                .opt_str("brokers")
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_else(|| vec!["localhost:9092".into()]);
            if brokers.is_empty() {
                return Err("Invalid --brokers specified!".to_owned());
            }

            Ok(Config { brokers: brokers })
        }
    }
}

#[cfg(not(feature = "security"))]
mod example {
    use std::process;

    pub fn main() {
        println!("example relevant only with the \"security\" feature enabled!");
        process::exit(1);
    }
}
