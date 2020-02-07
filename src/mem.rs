use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use lazy_static::lazy_static;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;
use tonic::Status;

use crate::proximo::{Confirmation, Message, Offset};

lazy_static! {
    static ref TOPICS: Arc<Mutex<HashMap<String, Sender<Result<Command, Status>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

pub async fn consume<ConfirmStream>(
    topic: &str,
    _consumer: &str,
    initial_offset: Offset,
    mut tx: Sender<Result<Message, Status>>,
    mut rx: ConfirmStream,
) where
    ConfirmStream: Stream<Item = Result<Confirmation, Status>> + Send + 'static + Unpin,
{
    let topics = TOPICS.lock().await;

    let mut sender = topics
        .get(topic)
        .ok_or_else(|| format!("unknown topic: {}", &topic))
        .unwrap()
        .clone();

    let (msg_tx, mut msg_rx) = mpsc::channel(4);

    if let Err(e) = sender
        .send(Ok(Command::Sub(Subscriber {
            initial_offset,
            sender: msg_tx,
        })))
        .await
    {
        panic!(e);
    }

    tokio::spawn(async move {
        while let Some(m) = msg_rx.next().await {
            let m = m.unwrap();

            if let Err(e) = tx.send(Ok(m)).await {
                panic!(e);
            }
        }
    });

    tokio::spawn(async move {
        while let Some(c) = rx.next().await {
            c.unwrap();
        }
    });
}

pub async fn publish<MessageStream>(
    topic: &str,
    mut tx: Sender<Result<Confirmation, Status>>,
    mut rx: MessageStream,
) where
    MessageStream: Stream<Item = Result<Message, Status>> + Send + 'static + Unpin,
{
    let t = topic.to_owned();

    let mut topics = TOPICS.lock().await;

    let mut sender = topics
        .entry(topic.to_owned())
        .or_insert_with(|| start_topic(&t))
        .clone();

    let (ack_tx, mut ack_rx) = mpsc::channel(4);

    tokio::spawn(async move {
        while let Some(m) = rx.next().await {
            let m = m.unwrap();

            if let Err(e) = sender.send(Ok(Command::Write((m, ack_tx.clone())))).await {
                panic!(e);
            }
        }
    });

    tokio::spawn(async move {
        while let Some(m) = ack_rx.next().await {
            let msg_id = m.unwrap();

            if let Err(e) = tx.send(Ok(Confirmation { msg_id })).await {
                panic!(e);
            }
        }
    });
}

struct Subscriber {
    initial_offset: Offset,
    sender: Sender<Result<Message, Status>>,
}

enum Command {
    Sub(Subscriber),
    Write((Message, Sender<Result<String, Status>>)),
}

fn start_topic(_topic: &str) -> Sender<Result<Command, Status>> {
    let (tx, mut rx) = mpsc::channel(4);

    tokio::spawn(async move {
        let mut subscribers = Vec::new();
        let mut messages: Vec<Message> = Vec::new();

        while let Some(m) = rx.next().await {
            match m {
                Ok(Command::Sub(mut sub)) => {
                    if sub.initial_offset == Offset::Oldest {
                        for m in &messages {
                            match sub.sender.send(Ok(m.clone())).await {
                                Ok(_) => (),
                                Err(_) => (),
                            }
                        }
                    }
                    subscribers.push(sub);
                }
                Ok(Command::Write((m, mut tx))) => {
                    for sub in subscribers.iter_mut() {
                        match sub.sender.send(Ok(m.clone())).await {
                            Ok(_) => (),
                            Err(_) => (),
                        }
                    }
                    messages.push(m.clone());
                    tx.send(Ok(m.id.to_owned())).await.unwrap();
                }
                Err(e) => panic!(e),
            }
        }
    });

    tx
}
