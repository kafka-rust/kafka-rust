fn main() {
    example::main();
}

#[cfg(feature = "security")]
mod example {
    extern crate openssl;
    extern crate kafka;
    extern crate getopts;
    extern crate env_logger;

    use std::env;
    use std::process;

    use self::kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    use self::openssl::ssl::{SslContext, SslMethod, SSL_VERIFY_PEER};
    use self::openssl::x509::X509FileType;

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

        // ~ OpenSSL offers a variety of complex configurations. Here is an example:
        let mut ctx = SslContext::new(SslMethod::Sslv23).unwrap();
        ctx.set_cipher_list("DEFAULT").unwrap();
        ctx.set_verify(SSL_VERIFY_PEER);
        if let Some(ccert) = cfg.client_cert {
            ctx.set_certificate_file(ccert, X509FileType::PEM).unwrap();
        }
        if let Some(ckey) = cfg.client_key {
            ctx.set_private_key_file(ckey, X509FileType::PEM).unwrap();
        }
        if let Some(ca_cert) = cfg.ca_cert {
            ctx.set_CA_file(ca_cert).unwrap();
        } else {
            // ~ allow client specify the CAs through the default paths:
            // "These locations are read from the SSL_CERT_FILE and
            // SSL_CERT_DIR environment variables if present, or defaults
            // specified at OpenSSL build time otherwise."
            ctx.set_default_verify_paths().unwrap();
        }

        // ~ instantiate KafkaClient with the previous OpenSSL setup
        let mut client = KafkaClient::new_secure(cfg.brokers, SecurityConfig::new(ctx));

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
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
            opts.optopt("", "ca-cert", "Specify the trusted CA certificates", "FILE");
            opts.optopt("", "client-cert", "Specify the client certificate", "FILE");
            opts.optopt("", "client-key", "Specify key for the client certificate", "FILE");

            let args: Vec<_> = env::args().collect();
            let m = match opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(e) => return Err(format!("{}", e)),
            };

            if m.opt_present("help") {
                let brief = format!("{} [options]", args[0]);
                return Err(opts.usage(&brief));
            };

            let brokers = m.opt_str("brokers")
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
                brokers: brokers,
                client_cert: m.opt_str("client-cert"),
                client_key: m.opt_str("client-key"),
                ca_cert: m.opt_str("ca-cert"),
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
