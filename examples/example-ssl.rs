fn main() {
    example::main();
}

#[cfg(feature = "security")]
mod example {
    extern crate native_tls;
    extern crate kafka;
    extern crate getopts;
    extern crate env_logger;

    use std::io::prelude::*;
    use std::fs::File;
    use std::env;
    use std::process;

    use self::kafka::client::{FetchOffset, KafkaClient, SecurityConfig};

    use self::native_tls::{TlsConnector, Protocol, Certificate, Pkcs12};

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

        // ~ NativeTls offers a variety of complex configurations. Here is an example:

        let mut builder = TlsConnector::builder().unwrap();

        builder
            .supported_protocols(&[Protocol::Tlsv11, Protocol::Tlsv12])
            .unwrap();

        if let Some(ca_cert) = cfg.ca_cert {
            let mut buf = vec![];
            File::open(ca_cert)
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            builder
                .add_root_certificate(Certificate::from_der(&buf).unwrap())
                .unwrap();
        };

        if let (Some(ccert), Some(passwd)) = (cfg.client_cert, cfg.client_pass) {
            let mut buf = vec![];
            File::open(ccert)
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            builder
                .identity(Pkcs12::from_der(&buf, &passwd).unwrap())
                .unwrap();
        }

        let connector = builder.build().unwrap();

        // ~ instantiate KafkaClient with the previous OpenSSL setup
        let mut client = KafkaClient::new_secure(cfg.brokers, SecurityConfig::new(connector));

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
        client_pass: Option<String>,
        ca_cert: Option<String>,
    }

    impl Config {
        fn from_cmdline() -> Result<Config, String> {
            let mut opts = getopts::Options::new();
            opts.optflag("h", "help", "Print this help screen");
            opts.optopt("", "brokers", "Specify kafka brokers (comma separated)", "HOSTS");
            opts.optopt("", "ca-cert", "Specify the trusted CA certificates in DER format", "FILE");
            opts.optopt("",
                        "client-cert",
                        "Specify the client certificate in PKCS12 format",
                        "FILE");
            opts.optopt("", "client-pass", "Specify password for the client certificate", "FILE");

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
                   client_pass: m.opt_str("client-pass"),
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
