extern crate time;
extern crate kafka;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;
extern crate stopwatch;

use std::collections::HashMap;
use std::env;
use std::str;
use std::time::Duration;

use kafka::error::Error as KafkaError;
use kafka::client::{KafkaClient, Compression, FetchOffset,
                    FetchPartition, PartitionOffset, RequiredAcks};
use kafka::consumer::Consumer;
use kafka::producer::{self, Producer};
use stopwatch::Stopwatch;

fn main() {
    env_logger::init().unwrap();

    let (cfg, cmds) = match Config::from_cmdline() {
        Err(e) => {
            println!("{}", e.trim_right());
            return;
        }
        Ok(m) => m,
    };

    for cmd in cmds {
        let cmd: &str = &cmd;
        let res = match cmd {
            "produce" => produce_data(&cfg),
            "consume" => consume_data(&cfg),
            "produce-consume-integration" => produce_consume_integration(&cfg),
            _ => Err(Error::Other(format!("unknown command: {} \
                                           [supported: \
                                           produce, \
                                           consume, \
                                           produce-consume-integration",
                                          cmd))),
        };
        if let Err(e) = res {
            println!("Error: {:?}", e);
        }
    }
}

#[derive(Debug)]
struct Config {
    brokers: Vec<String>,
    topics: Vec<String>,

    produce_msg_per_topic: u32,
    produce_bytes_per_msg: u32,
    produce_required_acks: RequiredAcks,
    produce_ack_timeout: Duration,

    compression: Compression,

    fetch_max_wait_time: Duration,
    fetch_min_bytes: i32,
    fetch_max_bytes: i32,
    fetch_crc_validation: bool,

    dump_consumed: bool,
    dump_offset: FetchOffset,
}

impl Config {
    fn from_cmdline() -> Result<(Config, Vec<String>), String> {
        let args: Vec<_> = env::args().collect();

        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify brokers (comma separated)", "HOSTS");
        opts.optopt("", "topics", "Specify topics (comma separated)", "TOPICS");
        opts.optopt("", "produce-msgs-per-topic", "Produce N messages per topic", "N");
        opts.optopt("", "produce-bytes-per-msg", "Produce N bytes per message", "N");
        opts.optopt("", "produce-required-acks", "Required acks for produced messages [NONE, ONE, ALL]", "TYPE");
        opts.optopt("", "produce-ack-timeout", "Allow acks to take this long", "MILLIS");
        opts.optopt("", "compression", "Set compression type [NONE, GZIP, SNAPPY]", "TYPE");
        opts.optflag("", "earliest-offset", "When dumping offsets use the earliest");
        opts.optflag("", "dump-consumed", "Print consumed message as utf8 strings");
        opts.optopt("", "fetch-max-wait-time", "Set the fetch-max-wait-time", "MILLIS");
        opts.optopt("", "fetch-min-bytes", "Set the fetch-min-bytes", "N");
        opts.optopt("", "fetch-max-bytes", "Set the fetch-max-bytes (per partition!)", "N");
        opts.optflag("", "fetch-no-crc-validation", "Do not validate checksums of fetched messages");
        let matches = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(e) => return Err(e.to_string()),
        };
        if matches.opt_present("help") {
            let brief = format!("{} [options]", args[0]);
            return Err(opts.usage(&brief));
        }
        let cfg = Config {
            brokers: matches.opt_str("brokers")
                .unwrap_or_else(|| "localhost:9092".to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            topics: matches.opt_str("topics")
                .map(|s| s.split(',').map(|s| s.trim().to_owned()).collect())
                .unwrap_or_else(|| vec![]),
            produce_msg_per_topic:
            try!(matches.opt_str("produce-msgs-per-topic")
                 .unwrap_or_else(|| "100000".to_owned())
                 .parse::<u32>()
                 .map_err(|e| format!("not a number: {}", e))),
            produce_bytes_per_msg:
            try!(matches.opt_str("produce-bytes-per-msg")
                 .unwrap_or_else(|| "10".to_owned())
                 .parse::<u32>()
                 .map_err(|e| format!("not a number: {}", e))),
            produce_required_acks: match matches.opt_str("produce-required-acks") {
                None => RequiredAcks::All,
                Some(s) => match s.trim() {
                    "none" | "NONE" => RequiredAcks::None,
                    "one" | "ONE" => RequiredAcks::One,
                    "all" | "ALL" => RequiredAcks::All,
                    _ => return Err(format!("Unknown required acks: {}", s)),
                },
            },
            produce_ack_timeout: Duration::from_millis(
                match matches.opt_str("produce-ack-timeout") {
                    None => producer::DEFAULT_ACK_TIMEOUT_MILLIS,
                    Some(s) => match s.parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => return Err(format!("Not a number: {}", s)),
                    }
                }),
            compression: {
                let s = matches.opt_str("compression").unwrap_or_else(|| "NONE".to_owned());
                match s.trim() {
                    "none" | "NONE" => Compression::NONE,
                    "gzip" | "GZIP" => Compression::GZIP,
                    "snappy" | "SNAPPY" => Compression::SNAPPY,
                    _ => return Err(format!("Unknown compression type: {}", s)),
                }
            },
            dump_consumed: matches.opt_present("dump-consumed"),
            fetch_max_wait_time:
            Duration::from_millis(
                try!(matches.opt_str("fetch-max-wait-time")
                     .unwrap_or_else(|| format!("{}", kafka::client::DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS))
                     .parse::<u64>()
                     .map_err(|e| format!("not a number: {}", e)))),
            fetch_min_bytes:
            try!(matches.opt_str("fetch-min-bytes")
                 .unwrap_or_else(|| format!("{}", kafka::client::DEFAULT_FETCH_MIN_BYTES))
                 .parse::<i32>()
                 .map_err(|e| format!("not a number: {}", e))),
            fetch_max_bytes:
            try!(matches.opt_str("fetch-max-bytes")
                 .unwrap_or_else(|| format!("{}", kafka::client::DEFAULT_FETCH_MAX_BYTES_PER_PARTITION))
                 .parse::<i32>()
                 .map_err(|e| format!("not a number: {}", e))),
            fetch_crc_validation: !matches.opt_present("fetch-no-crc-validation"),
            dump_offset: if matches.opt_present("earliest-offset") {
                FetchOffset::Earliest
            } else {
                FetchOffset::Latest
            },
        };
        let cmds = if matches.free.is_empty() {
            vec!["dump-offsets".to_owned()]
        } else {
            matches.free
        };
        Ok((cfg, cmds))
    }

    fn new_client(&self) -> Result<KafkaClient, Error> {
        let mut client = KafkaClient::new(self.brokers.iter().cloned().collect());

        client.set_compression(self.compression);
        debug!("Set client compression: {:?}", self.compression);

        client.set_fetch_max_wait_time(self.fetch_max_wait_time).expect("invalid fetch max time");
        debug!("Set client fetch-max-wait-time: {:?}", self.fetch_max_wait_time);

        client.set_fetch_min_bytes(self.fetch_min_bytes);
        debug!("Set client fetch-min-bytes: {:?}", self.fetch_min_bytes);

        client.set_fetch_max_bytes_per_partition(self.fetch_max_bytes);
        debug!("Set client fetch-max-bytes-per-partition: {:?}", self.fetch_max_bytes);

        client.set_fetch_crc_validation(self.fetch_crc_validation);
        debug!("Set client fetch-crc-validation: {}", self.fetch_crc_validation);

        try!(client.load_metadata_all());
        if !self.topics.is_empty() {
            let client_topics = client.topics();
            for topic in &self.topics {
                if !client_topics.contains(topic) {
                    return Err(Error::Other(format!("No such topic: {}", topic)));
                }
            }
        }
        Ok(client)
    }
}

#[derive(Debug)]
enum Error {
    Kafka(kafka::error::Error),
    Other(String),
}

impl From<kafka::error::Error> for Error {
    fn from(e: kafka::error::Error) -> Self { Error::Kafka(e) }
}

impl<'a> From<&'a str> for Error {
    fn from(s: &'a str) -> Self { Error::Other(s.to_owned()) }
}

fn produce_data(cfg: &Config) -> Result<(), Error> {
    use std::borrow::ToOwned;

    debug!("producing data to: {:?}", cfg);

    let msg: Vec<u8> = (0..cfg.produce_bytes_per_msg)
        .map(|v| (v % 256) as u8)
        .collect();

    let client = try!(cfg.new_client());
    let topics: Vec<String> =
        if cfg.topics.is_empty() {
            client.topics().names().map(ToOwned::to_owned).collect()
        } else {
            cfg.topics.clone()
        };

    let msg_total = cfg.produce_msg_per_topic as usize * topics.len();
    let mut data = Vec::with_capacity(msg_total);
    for topic in &topics {
        for _ in 0 .. cfg.produce_msg_per_topic {
            data.push(producer::Record::from_value(&topic, &*msg));
        }
    }
    debug!("data.len() = {}", data.len());

    let mut producer = try!(Producer::from_client(client)
                            .with_ack_timeout(cfg.produce_ack_timeout)
                            .with_required_acks(cfg.produce_required_acks)
                            .create());
    let sw = Stopwatch::start_new();
    let cfrms = try!(producer.send_all(&data));
    let elapsed_ms = sw.elapsed_ms();
    debug!("Sent {} messages in {}ms ==> {:.2} msg/s ==> {:.2} bytes/s",
           msg_total, elapsed_ms,
           (1000 * msg_total) as f64 / elapsed_ms as f64,
           (1000 * msg_total * msg.len()) as f64 / elapsed_ms as f64);

    // ~ validate whether we successfully sent the messages to all the target partitions
    for cfrm in cfrms {
        for pc in cfrm.partition_confirms {
            if let Err(e) = pc.offset {
                return Err(From::from(KafkaError::Kafka(e)));
            }
        }
    }

    // ~ everything fine :)
    Ok(())
}

fn consume_data(cfg: &Config) -> Result<(), Error> {
    let mut client = try!(cfg.new_client());

    let topics: Vec<String> =
        if cfg.topics.is_empty() {
            client.topics().names().map(ToOwned::to_owned).collect()
        } else {
            cfg.topics.clone()
        };
    for topic in &topics {
        let mut consumer = try!(Consumer::from_client(client)
                                .with_topic(topic.to_owned())
                                .with_fallback_offset(FetchOffset::Earliest)
                                .create());

        // ~ now request all the data from the topic
        let sw = Stopwatch::start_new();
        let (mut n_bytes, mut n_msgs, n_errors) = (0u64, 0u64, 0u64);
        loop {
            trace!("Issueing fetch_messages request");
            let ms = try!(consumer.poll());
            if ms.is_empty() {
                break;
            }

            for msg in ms.iter().flat_map(|m| m.messages()) {
                let msg_val = msg.value;

                if cfg.dump_consumed {
                    match str::from_utf8(msg_val) {
                        Ok(s) => println!("{}", s),
                        Err(e) => warn!("Failed decoding message as utf8 string: {}", e),
                    }
                }

                n_msgs += 1;
                n_bytes += msg_val.len() as u64;

                if n_msgs % 500_000 == 0 {
                    let elapsed_ms = sw.elapsed_ms();
                    let total = n_msgs + n_errors;
                    debug!("topic: {}, total msgs: {} (errors: {}), bytes: {}, elapsed: {}ms ==> msg/s: {:.2} (bytes/s: {:.2})",
                           topic, total, n_errors, n_bytes, elapsed_ms,
                           (1000 * total) as f64 / elapsed_ms as f64,
                           (1000 * n_bytes) as f64 / elapsed_ms as f64);
                }
            }
        }
        let elapsed_ms = sw.elapsed_ms();

        let total = n_msgs + n_errors;
        debug!("topic: {}, total msgs: {} (errors: {}), bytes: {}, elapsed: {}ms ==> msg/s: {:.2} (bytes/s: {:.2})",
               topic, total, n_errors, n_bytes, elapsed_ms,
               (1000 * total) as f64 / elapsed_ms as f64,
               (1000 * n_bytes) as f64 / elapsed_ms as f64);

        client = consumer.into_client();
    }
    Ok(())
}

/// Produces messages to a topic and reads them back. Verifying none
/// has been lost. Assumes no concurrent producers to the target
/// topic.
fn produce_consume_integration(cfg: &Config) -> Result<(), Error> {
    fn do_test(mut client: KafkaClient, topics: &[String], cfg: &Config, sent_msg: &[u8])
               -> Result<(), Error>
    {
        // ~ remeber the current offsets
        let init_offsets = try!(client.fetch_offsets(topics, FetchOffset::Latest));
        trace!("init_offsets: {:#?}", init_offsets);

        // ~ produce data to the target topics
        {
            // ~ the data set will be sending
            let msgs = {
                let mut msgs = Vec::with_capacity(topics.len() * cfg.produce_msg_per_topic as usize);
                for topic in topics {
                    for _ in 0..cfg.produce_msg_per_topic as usize {
                        msgs.push(producer::Record::from_value(topic, sent_msg));
                    }
                }
                msgs
            };

            // ~ send the messages
            let mut producer = try!(Producer::from_client(client)
                                    .with_ack_timeout(cfg.produce_ack_timeout)
                                    .with_required_acks(cfg.produce_required_acks)
                                    .create());
            try!(producer.send_all(&msgs));
            debug!("Sent {} messages", msgs.len());

            // ~ get back the client
            client = producer.into_client();
        }

        // ~ verify the messages
        let msgs_per_topic = try!(verify_messages(&mut client, init_offsets, sent_msg));
        for (_, v) in msgs_per_topic {
            assert_eq!(v, cfg.produce_msg_per_topic as usize);
        }
        debug!("Verified all fetched messages");

        Ok(())
    }

    // consumes all available message for the topic partitions as of
    // the specified offsets, verifies they equal to the specified
    // `needle`, and counts the number of messages per partition
    // retrieved.
    fn verify_messages(client: &mut KafkaClient,
                       start_offsets: HashMap<String, Vec<PartitionOffset>>,
                       needle: &[u8])
                       -> kafka::error::Result<HashMap<String, usize>>
    {
        let mut offs = HashMap::new();
        for (topic, pos) in &start_offsets {
            for po in pos {
                offs.insert(format!("{}-{}", topic, po.partition),
                            FetchPartition::new(&topic, po.partition, po.offset));
            }
        }

        let mut counts = HashMap::with_capacity(start_offsets.len());
        for (topic, _) in &start_offsets {
            counts.insert(topic.to_owned(), 0);
        }
        loop {
            // ~ now fetch the messages and verify we could read them back
            let mut had_messages = false;
            for r in try!(client.fetch_messages(offs.values())).iter() {
                for t in r.topics() {
                    for p in t.partitions() {
                        assert!(p.data().is_ok());
                        let msgs = p.data().as_ref().unwrap().messages();
                        for m in msgs.iter() {
                            assert_eq!(m.value, needle);
                        }
                        if msgs.len() > 0 {
                            had_messages = true;
                            *counts.get_mut(t.topic()).unwrap() += msgs.len();
                            offs.get_mut(&format!("{}-{}", t.topic(), p.partition())).unwrap().offset =
                                msgs.last().unwrap().offset + 1;
                        }
                    }
                }
            }
            if !had_messages {
                break;
            }
        }
        Ok(counts)
    }

    // ~ --------------------------------------------------------------

    if cfg.topics.is_empty() {
        return Err(Error::Other("At least one topic must be explicitely provided!".to_owned()));
    }

    // ~ make sure all of the topics we will be sending messages to do exist
    let mut client = try!(cfg.new_client());
    try!(client.load_metadata_all());
    {
        let client_topics = client.topics();
        for topic in &cfg.topics {
            if !client_topics.contains(topic) {
                return Err(Error::Other(format!("Non existent topic: {}", topic)));
            }
        }
    }

    // ~ our test messages
    let s = b"hello, world";
    let sent_msg: Vec<u8> = s.into_iter().cycle().take(cfg.produce_bytes_per_msg as usize).cloned().collect();
    // ~ run the test
    try!(do_test(client, &cfg.topics, cfg, &sent_msg));

    Ok(())
}
