extern crate kafka;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;
extern crate stopwatch;

use std::cmp;
use std::collections::HashMap;
use std::env;
use std::iter::repeat;

use kafka::client::{KafkaClient, Compression};
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
            "dump-offsets" => print_offsets(&cfg),
            "produce" => produce_data(&cfg),
            "consume" => consume_data(&cfg),
            "test-produce-consume-integration" => test_produce_consume_integration(&cfg),
            _ => Err(Error::Other(format!("unknown command: {}", cmd))),
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

    compression: Compression,
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
        opts.optopt("", "compression", "Set compression type [NONE, GZIP, SNAPPY]", "TYPE");
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
            compression: {
                let s = matches.opt_str("compression").unwrap_or_else(|| "NONE".to_owned());
                match s.trim() {
                    "none" | "NONE" => Compression::NONE,
                    "gzip" | "GZIP" => Compression::GZIP,
                    "snappy" | "SNAPPY" => Compression::SNAPPY,
                    _ => return Err(format!("Unknown compression type: {}", s)),
                }
            },
        };
        let cmds = if matches.free.is_empty() {
            vec!["offsets".to_owned()]
        } else {
            matches.free
        };
        Ok((cfg, cmds))
    }

    fn new_client(&self) -> Result<KafkaClient, Error> {
        let mut client = KafkaClient::new(self.brokers.iter().cloned().collect());
        if self.topics.is_empty() {
            try!(client.load_metadata_all());
        } else {
            try!(client.load_metadata(self.topics.iter().cloned().collect()));
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

fn print_offsets(cfg: &Config) -> Result<(), Error> {
    debug!("printing offsets for: {:?}", cfg);

    let mut client = try!(cfg.new_client());
    let topics = client.topic_partitions.keys().cloned().collect();
    let offs = try!(client.fetch_offsets(topics, -1));
    if offs.is_empty() {
        return Ok(());
    }

    let topic_width = cmp::max(30, offs.keys().map(|s| s.len()).max().unwrap() + 2);
    println!("{:3$} {:>10} {:>12}", "topic", "partition", "offset", topic_width-8);
    println!("{:3$} {:>10} {:>12}", "=====", "=========", "======", topic_width-8);

    let mut offs: Vec<_> = offs.into_iter().collect();
    offs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut i = 0;
    for (topic, mut offsets) in offs {
        if i != 0 { println!(""); }
        i += 1;

        offsets.sort_by(|a, b| a.partition.cmp(&b.partition));
        for off in offsets {
            println!("{:3$} {:>2} {:>12}", topic, off.partition, off.offset, topic_width);
        }
    }
    Ok(())
}

fn produce_data(cfg: &Config) -> Result<(), Error> {
    debug!("producing data to: {:?}", cfg);

    let msg: Vec<u8> = (0..cfg.produce_bytes_per_msg).map(|v| (v % 256) as u8).collect();

    let mut client = try!(cfg.new_client());

    let msg_total = cfg.produce_msg_per_topic as usize * client.topic_partitions.len();
    let mut data = Vec::with_capacity(msg_total);
    for _ in 0..cfg.produce_msg_per_topic {
        for topic in client.topic_partitions.keys().cloned() {
            data.push(kafka::utils::ProduceMessage{
                topic: topic,
                message: msg.clone(),
            });
        }
    }
    let sw = Stopwatch::start_new();
    //try!(client.send_messages(-1, 1000, data));
    try!(client.send_messages(0, 0, data));
    let elapsed_ms = sw.elapsed_ms();
    debug!("Sent {} messages in {}ms ==> {:.2} msg/s",
           msg_total, elapsed_ms, (1000 * msg_total) as f64 / elapsed_ms as f64);

    Ok(())
}

fn consume_data(cfg: &Config) -> Result<(), Error> {
    let mut client = try!(cfg.new_client());

    let topic_partitions = client.topic_partitions.clone();

    for (topic, partitions) in topic_partitions {
        let mut offsets: Vec<i64> = {
            let max_id = partitions.iter().fold(0, |a, b| cmp::max(a, *b));
            repeat(0).take((max_id + 1) as usize).collect()
        };
        // ~ initialize start offsets
        {
            let start_offset = try!(client.fetch_topic_offset(topic.clone(), -2));
            match start_offset.get(&topic) {
                Some(poffs) => {
                    for poff in poffs {
                        offsets[poff.partition as usize] = poff.offset;
                        debug!("{}:{} => start_offset: {}",
                               &topic, poff.partition, poff.offset);
                    }
                },
                None => {
                    debug!("Could not determined earliest offset for topic: {}",
                           topic.clone());
                    continue;
                }
            };
        }

        let sw = Stopwatch::start_new();
        let (mut n_bytes, mut n_msgs, mut n_errors) = (0u64, 0u64, 0u64);
        loop {
            let mut reqs = Vec::with_capacity(partitions.len());
            for p in &partitions {
                reqs.push(kafka::utils::TopicPartitionOffset{
                    topic: topic.clone(),
                    partition: *p,
                    offset: offsets[*p as usize],
                });
            }

            let msgs = try!(client.fetch_messages_multi(reqs));
            if msgs.is_empty() {
                break;
            }

            for msg in msgs {
                trace!("msg.topic: {}, msg.partition: {}, msg.offset: {}",
                       msg.topic, msg.partition, msg.offset);
                match msg.error {
                    Some(_) => {
                        n_errors += 1;
                    }
                    None => {
                        n_msgs += 1;
                        n_bytes += msg.message.len() as u64;
                        offsets[msg.partition as usize] += 1;
                        if n_msgs % 100000 == 0 {
                            let elapsed_ms = sw.elapsed_ms();
                            let total = n_msgs + n_errors;
                            debug!("topic: {}, total msgs: {} (errors: {}), bytes: {}, elapsed: {}ms ==> msg/s: {:.2}",
                                   topic, total, n_errors, n_bytes, elapsed_ms,
                                   (1000 * total) as f64 / elapsed_ms as f64);
                        }
                    }
                }
            }
        }
        let elapsed_ms = sw.elapsed_ms();

        let total = n_msgs + n_errors;
        debug!("topic: {}, total msgs: {} (errors: {}), bytes: {}, elapsed: {}ms ==> msg/s: {:.2}",
               topic, total, n_errors, n_bytes, elapsed_ms,
               (1000 * total) as f64 / elapsed_ms as f64);
    }
    Ok(())
}

/// Produces messages to a topic and reads them back. Verifying none
/// has been lost. Assumes no concurrent producers to the target
/// topic.
fn test_produce_consume_integration(cfg: &Config) -> Result<(), Error> {

    fn do_test(client: &mut KafkaClient, topics: &[String], msg_per_topic: usize, sent_msg: &[u8]) -> Result<(), Error> {
        // ~ the data set will be sending
        let msgs = {
            let mut msgs = Vec::with_capacity(topics.len() * msg_per_topic as usize);
            for topic in topics {
                for _ in 0..msg_per_topic {
                    msgs.push(kafka::utils::ProduceMessage { topic: topic.to_owned(), message: sent_msg.to_owned() });
                }
            }
            msgs
        };

        // ~ remeber the current offsets
        let init_offsets = try!(client.fetch_offsets(topics.iter().cloned().collect(), -1));
        debug!("init_offsets: {:#?}", init_offsets);

        // ~ send the messages
        let n_msgs = msgs.len();
        try!(client.send_messages(1, 1000, msgs));
        debug!("Sent {} messages", n_msgs);

        // ~ now fetch the messages and verify we could read them back
        let mut reqs = vec![];
        for topic in topics {
            let init_offsets = init_offsets.get(topic).unwrap();
            let partitions = client.topic_partitions.get(topic).unwrap();
            for partition in partitions {
                reqs.push(kafka::utils::TopicPartitionOffset {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: find_offset(init_offsets, *partition).unwrap(),
                });
            }
        }

        debug!("Fetching messages for: {:#?}", reqs);
        let fetched_msgs = try!(client.fetch_messages_multi(reqs));
        debug!("Fetched {} messages", fetched_msgs.len());
        let mut msgs_per_topic = HashMap::with_capacity(topics.len());
        for msg in fetched_msgs {
            assert_eq!(msg.message, sent_msg);
            if let Some(v) = msgs_per_topic.get_mut(&msg.topic) {
                *v = *v + 1;
                continue;
            }
            msgs_per_topic.insert(msg.topic.clone(), 1);
        }
        for (_, v) in msgs_per_topic {
            assert_eq!(v, msg_per_topic);
        }
        debug!("Verified all fetched messages");
        Ok(())
    }

    fn find_offset(offsets: &[kafka::utils::PartitionOffset], partition: i32) -> Option<i64> {
        offsets.into_iter()
            .filter(|off| off.partition == partition)
            .next()
            .and_then(|off| Some(off.offset))
    }

    // ~ --------------------------------------------------------------

    if cfg.topics.is_empty() {
        return Err(Error::Other("At least one topic must be explicitely provided!".to_owned()));
    }

    // ~ make sure all of the topics we will be sending messages to do exist
    let mut client = try!(cfg.new_client());
    try!(client.load_metadata_all());
    for topic in &cfg.topics {
        if !client.topic_partitions.contains_key(topic) {
            return Err(Error::Other(format!("Non existent topic: {}", topic)));
        }
    }

    // ~ our test messages
    let s = b"hello, world";
    let sent_msg: Vec<u8> = s.into_iter().cycle().take(cfg.produce_bytes_per_msg as usize).cloned().collect();
    // ~ run the test
    try!(do_test(&mut client, &cfg.topics, cfg.produce_msg_per_topic as usize, &sent_msg));

    Ok(())
}
