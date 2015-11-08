extern crate time;
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
use std::str;
use std::thread;

use kafka::client::{KafkaClient, Compression, FetchOffset};
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
            "follow-offsets" => follow_offsets(&cfg),
            "dump-offsets" => dump_offsets(&cfg),
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

    dump_consumed: bool,
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
        opts.optflag("", "dump-consumed", "Print consumed message as utf8 strings");
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
            dump_consumed: matches.opt_present("dump-consumed"),
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
        try!(client.load_metadata_all());

        if !self.topics.is_empty() {
            for topic in &self.topics {
                if !client.topic_partitions.contains_key(topic) {
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

fn follow_offsets(cfg: &Config)-> Result<(), Error> {
    use std::io::stdout;
    use std::fmt::Write;

    debug!("following offsets for: {:?}", cfg);


    if cfg.topics.len() != 1 {
        return Err(Error::from("Following offsets is only supported for exactly one topic!"));
    }

    let mut client = try!(cfg.new_client());

    let mut out = String::with_capacity(160);
    let mut stdout = stdout();
    let topic = &cfg.topics[0];

    loop {
        let now = time::now();
        let mut offs = try!(client.fetch_topic_offset(topic, FetchOffset::Latest));
        offs.sort_by(|a, b| a.partition.cmp(&b.partition));
        debug!("fetched offsets: {:?}", offs);

        out.clear();
        let _ = write!(out, "{}", now.strftime("%H:%M:%S").unwrap());
        for off in offs {
            let _ = match off.offset {
                Ok(o) => write!(out, " {:>10}", o),
                Err(_) => write!(out, " {:>10}", "ERR"),
            };
        }
        let _ = write!(out, "\n");

        {
            use std::io::Write;
            let _ = stdout.write_all(out.as_bytes());
        }

        {
            use std::time;
            thread::sleep(time::Duration::from_millis(1000));
        }
    }
}

fn dump_offsets(cfg: &Config) -> Result<(), Error> {
    debug!("dumping offsets for: {:?}", cfg);

    let mut client = try!(cfg.new_client());
    let topics: Vec<String> = if cfg.topics.is_empty() {
        client.topic_partitions.keys().cloned().collect()
    } else {
        cfg.topics.clone()
    };
    let offs = try!(client.fetch_offsets(&topics, FetchOffset::Latest));
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
            println!("{:3$} {:>2} {:>12}", topic, off.partition, off.offset.unwrap_or(-1), topic_width);
        }
    }
    Ok(())
}

fn produce_data(cfg: &Config) -> Result<(), Error> {
    debug!("producing data to: {:?}", cfg);

    let msg: Vec<u8> = (0..cfg.produce_bytes_per_msg).map(|v| (v % 256) as u8).collect();

    let mut client = try!(cfg.new_client());
    let topics: Vec<String> = if cfg.topics.is_empty() {
        client.topic_partitions.keys().cloned().collect()
    } else {
        cfg.topics.clone()
    };

    let msg_total = cfg.produce_msg_per_topic as usize * topics.len();
    let mut data = Vec::with_capacity(msg_total);
    for _ in 0..cfg.produce_msg_per_topic {
        for topic in topics.iter() {
            data.push(kafka::utils::ProduceMessage {
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

    let topic_partitions = if cfg.topics.is_empty() {
        client.topic_partitions.clone()
    } else {
        let mut m = HashMap::new();
        for topic in &cfg.topics {
            if let Some(partitions) = client.topic_partitions.get(&topic[..]) {
                m.insert(topic.clone(), partitions.clone());
            };
        }
        m
    };

    for (topic, partitions) in topic_partitions {
        let mut offsets: Vec<i64> = {
            let max_id = partitions.iter().fold(0, |a, b| cmp::max(a, *b));
            repeat(0).take((max_id + 1) as usize).collect()
        };
        // ~ initialize start offsets
        {
            let start_offset = try!(client.fetch_topic_offset(&topic, FetchOffset::Earliest));
            if start_offset.is_empty() {
                debug!("Could not determined earliest offset for topic: {}", topic);
            } else {
                for poff in start_offset {
                    match poff.offset {
                        Ok(off) => offsets[poff.partition as usize] = off,
                        Err(ref e) => {
                            offsets[poff.partition as usize] = -1;
                            warn!("{}:{} => error on fetching offset: {}",
                                  &topic, poff.partition, e);
                        }
                    }
                    debug!("{}:{} => start_offset: {:?}",
                           &topic, poff.partition, poff.offset);
                }
            }
        }

        let sw = Stopwatch::start_new();
        let (mut n_bytes, mut n_msgs, mut n_errors) = (0u64, 0u64, 0u64);
        loop {
            let mut reqs = Vec::with_capacity(partitions.len());
            for p in &partitions {
                let off = offsets[*p as usize];
                if off >= 0 {
                    reqs.push(kafka::utils::TopicPartitionOffset{
                        topic: &topic,
                        partition: *p,
                        offset: off
                    });
                }
            }

            let msgs = try!(client.fetch_messages_multi(reqs));
            if msgs.is_empty() {
                break;
            }

            for msg in msgs {
                trace!("msg.topic: {}, msg.partition: {}, msg.offset: {}, msg.error: {:?}",
                       msg.topic, msg.partition, msg.offset, msg.error);

                match msg.error {
                    Some(_) => {
                        n_errors += 1;
                    }
                    None => {
                        if cfg.dump_consumed {
                            match str::from_utf8(&msg.message) {
                                Ok(s) => println!("{}", s),
                                Err(e) => warn!("Failed decoding message as utf8 string: {}", e),
                            }
                        }

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
                    msgs.push(kafka::utils::ProduceMessage { topic: topic, message: sent_msg.to_owned() });
                }
            }
            msgs
        };

        // ~ remeber the current offsets
        let init_offsets = try!(client.fetch_offsets(topics, FetchOffset::Latest));
        trace!("init_offsets: {:#?}", init_offsets);

        // ~ send the messages
        let n_msgs = msgs.len();
        try!(client.send_messages(1, 1000, msgs));
        debug!("Sent {} messages", n_msgs);

        let fetched_msgs = try!(fetch_all_messages(client, init_offsets));
        debug!("Fetched {} messages", fetched_msgs.len());
        let mut msgs_per_topic = HashMap::with_capacity(topics.len());
        for msg in fetched_msgs {
            assert!(msg.error.is_none());
            assert_eq!(msg.message, sent_msg);
            if let Some(v) = msgs_per_topic.get_mut(&msg.topic) {
                *v = *v + 1;
                continue;
            }
            msgs_per_topic.insert(msg.topic, 1);
        }
        for (_, v) in msgs_per_topic {
            assert_eq!(v, msg_per_topic);
        }

        debug!("Verified all fetched messages");
        Ok(())
    }

    // just a hacky way to consume all available message for the topic
    // given partitions as of the specified offsets
    fn fetch_all_messages(client: &mut KafkaClient,
                          start_offsets: HashMap<String, Vec<kafka::utils::PartitionOffset>>)
                          -> kafka::error::Result<Vec<kafka::utils::TopicMessage>> {
        let mut offs = HashMap::new();
        for (topic, pos) in &start_offsets {
            for po in pos {
                offs.insert(format!("{}-{}", topic, po.partition),
                            kafka::utils::TopicPartitionOffset {
                                topic: &topic,
                                partition: po.partition,
                                offset: po.offset.clone().unwrap(),
                            });
            }
        }

        let mut resp = Vec::new();
        loop {
            // ~ now fetch the messages and verify we could read them back
            let msgs = {
                let reqs: Vec<_> = offs.values().collect();
                trace!("Fetching messages for: {:#?}", reqs);
                try!(client.fetch_messages_multi(reqs))
            };
            if msgs.is_empty() {
                return Ok(resp);
            }
            for msg in msgs {
                offs.get_mut(&format!("{}-{}", msg.topic, msg.partition)).unwrap().offset += 1;
                resp.push(msg);
            }
        }
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
