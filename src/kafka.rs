use std::collections::HashMap;

use futures::stream::{self, Stream, StreamExt};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::sync::mpsc::{self, Sender};
use tonic::Status;

use crate::proximo::{Confirmation, Message, Offset};

enum Command<'a> {
    Kafka(rdkafka::message::BorrowedMessage<'a>),
    Proximo(Confirmation),
}

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _rebalance: &Rebalance) {}

    fn post_rebalance(&self, _rebalance: &Rebalance) {}

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {}
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub fn consume<ConfirmStream>(
    servers: &str,
    topic: &str,
    consumer: &str,
    initial_offset: Offset,
    mut tx: Sender<Result<Message, Status>>,
    rx: ConfirmStream,
) where
    ConfirmStream: Stream<Item = Confirmation> + Send + 'static + Unpin,
{
    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", consumer)
        .set("bootstrap.servers", servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false");

    match initial_offset {
        Offset::Newest => {
            config.set("auto.offset.reset", "latest");
        }
        Offset::Oldest => {
            config.set("auto.offset.reset", "earliest");
        }
        _ => (),
    };

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topic.split(",").collect::<Vec<&str>>())
        .expect("Can't subscribe to specified topics");

    tokio::spawn(async move {
        let mut messages = HashMap::new();
        let message_stream = consumer
            .start()
            .map(|m| m.map(Command::Kafka).map_err(|e| e.to_string()));
        let rx = rx.map(|m| Ok(Command::Proximo(m)));

        let mut s = stream::select(message_stream, rx);

        while let Some(message) = s.next().await {
            match message {
                Ok(Command::Kafka(m)) => {
                    let payload = m.payload_view::<[u8]>().unwrap().unwrap();

                    if let Err(e) = tx
                        .send(Ok(Message {
                            id: m.offset().to_string(),
                            data: payload.into(),
                        }))
                        .await
                    {
                        println!("{}", e)
                    }
                    messages.insert(m.offset(), m);
                }
                Ok(Command::Proximo(c)) => {
                    let m = messages.remove(&c.msg_id.parse::<i64>().unwrap()).unwrap();
                    if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
                        println!("{}", e)
                    }
                }
                Err(e) => panic!(e),
            }
        }
    });
}

pub fn publish<MessageStream>(
    servers: &str,
    topic: &str,
    mut tx: Sender<Result<Confirmation, Status>>,
    mut rx: MessageStream,
) where
    MessageStream: Stream<Item = Message> + Send + 'static + Unpin,
{
    let topic = topic.to_owned();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let (mut ack_tx, mut ack_rx) = mpsc::channel(4);

    tokio::spawn(async move {
        while let Some((delivery_status, msg_id)) = ack_rx.next().await {
            match delivery_status.await {
                Ok(Ok((_, _))) => {
                    if let Err(e) = tx.send(Ok(Confirmation { msg_id })).await {
                        println!("{}", e);
                        return
                    }
                }
                e => {
                    println!("{:?}", e);
                    return
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(m) = rx.next().await {
            let delivery_status = producer.send(
                FutureRecord::to(&topic)
                    .payload(&m.data)
                    .key(&format!("Key {}", "0")),
                0,
            );

            if let Err(e) = ack_tx.send((delivery_status, m.id)).await {
                println!("{}", e);
                return
            }
        }
    });
}
