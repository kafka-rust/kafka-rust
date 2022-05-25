#[macro_use]
extern crate log;

fn main() {
    example::main();
}

#[cfg(feature = "security")]
mod example {
    use kafka;
    use openssl;

    use std::env;
    use std::process;

    use self::kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    use self::openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};

    pub fn main() {
        env_logger::init();

        // ~ parse the command line arguments
        let cfg = match Config::from_cmdline() {
            Ok(cfg) => cfg,
            Err(e) => {
                println!("{}", e);
                process::exit(1);
            }
        };

        // ~ OpenSSL offers a variety of complex configurations. Here is an example:
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_cipher_list("DEFAULT").unwrap();
        builder.set_verify(SslVerifyMode::PEER);
        if let (Some(ccert), Some(ckey)) = (cfg.client_cert, cfg.client_key) {
            info!("loading cert-file={}, key-file={}", ccert, ckey);

            builder
                .set_certificate_file(ccert, SslFiletype::PEM)
                .unwrap();
            builder
                .set_private_key_file(ckey, SslFiletype::PEM)
                .unwrap();
            builder.check_private_key().unwrap();
        }

        if let Some(ca_cert) = cfg.ca_cert {
            info!("loading ca-file={}", ca_cert);

            builder.set_ca_file(ca_cert).unwrap();
        } else {
            // ~ allow client specify the CAs through the default paths:
            // "These locations are read from the SSL_CERT_FILE and
            // SSL_CERT_DIR environment variables if present, or defaults
            // specified at OpenSSL build time otherwise."
            builder.set_default_verify_paths().unwrap();
        }

        let connector = builder.build();

        // ~ instantiate KafkaClient with the previous OpenSSL setup
        let mut client = KafkaClient::new_secure(
            cfg.brokers,
            SecurityConfig::new(connector).with_hostname_verification(cfg.verify_hostname),
        );

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
        client_cert: Option<String>,
        client_key: Option<String>,
        ca_cert: Option<String>,
        verify_hostname: bool,
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
            opts.optopt("", "ca-cert", "Specify the trusted CA certificates", "FILE");
            opts.optopt("", "client-cert", "Specify the client certificate", "FILE");
            opts.optopt(
                "",
                "client-key",
                "Specify key for the client certificate",
                "FILE",
            );
            opts.optflag(
                "",
                "no-hostname-verification",
                "Do not perform server hostname verification (insecure!)",
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

            Ok(Config {
                brokers,
                client_cert: m.opt_str("client-cert"),
                client_key: m.opt_str("client-key"),
                ca_cert: m.opt_str("ca-cert"),
                verify_hostname: !m.opt_present("no-hostname-verification"),
            })
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
