#![feature(convert)]

extern crate kafka;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;
extern crate stopwatch;

use std::cmp;
use std::env;
use kafka::client::KafkaClient;
use byteorder::WriteBytesExt;
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
        match cmd.as_str() {
            "offsets" => {
                if let Err(e) = print_offsets(&cfg) {
                    println!("error: {:?}", e)
                }
            }
            "produce" => {
                if let Err(e) = produce_data(&cfg) {
                    println!("error: {:?}", e);
                }
            }
            _ => {
                println!("unknown command: {}", cmd);
            }
        }
    }
}

#[derive(Debug)]
struct Config {
    brokers: Vec<String>,
    topics: Vec<String>,
}

impl Config {
    fn from_cmdline() -> Result<(Config, Vec<String>), String> {
        let args: Vec<_> = env::args().collect();

        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "Print this help screen");
        opts.optopt("", "brokers", "Specify brokers (comma separated)", "HOSTS");
        opts.optopt("", "topics", "Specify topics (comma separated)", "TOPICS");
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
    Kafka(kafka::error::Error)
}

impl From<kafka::error::Error> for Error {
    fn from(e: kafka::error::Error) -> Self { Error::Kafka(e) }
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
        let mut topic: &str = &topic;
        for off in offsets {
            println!("{:3$} {:>2} {:>12}", topic, off.partition, off.offset, topic_width);
            topic = "";
        }
    }
    Ok(())
}

fn produce_data(cfg: &Config) -> Result<(), Error> {
    let mut client = try!(cfg.new_client());

    let msg_per_topic = 100_000;
    let msg_total = msg_per_topic*client.topic_partitions.len();
    let mut data = Vec::with_capacity(msg_total);
    for i in (0 .. msg_per_topic) {
        for topic in client.topic_partitions.keys().cloned() {
            data.push(kafka::utils::ProduceMessage{
                topic: topic,
                message: encode(i as i64),
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

fn encode(n: i64) -> Vec<u8> {
    let mut wrt = Vec::with_capacity(8);
    wrt.write_i64::<byteorder::BigEndian>(n).unwrap();
    wrt
}
