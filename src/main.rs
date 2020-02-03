use argh::FromArgs;
use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use proximo::message_sink_server::{MessageSink, MessageSinkServer};
use proximo::message_source_server::{MessageSource, MessageSourceServer};
use proximo::{Confirmation, ConsumerRequest, Message, Offset, PublisherRequest};

mod kafka;

mod proximo {
    tonic::include_proto!("proximo");
}

#[derive(Debug)]
pub struct MessageSourceService {
    args: ReedArgs,
}

#[tonic::async_trait]
impl MessageSource for MessageSourceService {
    type ConsumeStream = mpsc::Receiver<Result<Message, Status>>;

    async fn consume(
        &self,
        request: Request<tonic::Streaming<ConsumerRequest>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let (ack_tx, ack_rx) = mpsc::channel(4);
        let mut stream = request.into_inner();
        let req = stream.next().await.unwrap().unwrap();
        let start = req.start_request.unwrap();

        let stream = stream.filter_map(|i| async move {
            if let Ok(ConsumerRequest {
                confirmation: Some(c),
                ..
            }) = i
            {
                Some(c)
            } else {
                None
            }
        });

        match &self.args.subcommand {
            SubCommands::Kafka(command) => {
                kafka::consume(
                    &command.servers,
                    &start.topic,
                    &start.consumer,
                    Offset::from_i32(start.initial_offset).unwrap(),
                    ack_tx,
                    Box::pin(stream),
                );
            }
        }

        Ok(Response::new(ack_rx))
    }
}

#[derive(Debug)]
pub struct MessageSinkService {
    args: ReedArgs,
}

#[tonic::async_trait]
impl MessageSink for MessageSinkService {
    type PublishStream = mpsc::Receiver<Result<Confirmation, Status>>;

    async fn publish(
        &self,
        request: Request<tonic::Streaming<PublisherRequest>>,
    ) -> Result<Response<Self::PublishStream>, Status> {
        let (ack_tx, ack_rx) = mpsc::channel(4);
        let mut stream = request.into_inner();
        let req = stream.next().await.unwrap().unwrap();
        let start = req.start_request.unwrap();

        let stream = stream.filter_map(|i| async move {
            if let Ok(PublisherRequest { msg: Some(m), .. }) = i {
                Some(m)
            } else {
                None
            }
        });

        match &self.args.subcommand {
            SubCommands::Kafka(command) => {
                kafka::publish(&command.servers, &start.topic, ack_tx, Box::pin(stream));
            }
        }

        Ok(Response::new(ack_rx))
    }
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
/// Reed: Proximo Server implementation written in Rust.
struct ReedArgs {
    #[argh(subcommand)]
    subcommand: SubCommands,
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
#[argh(subcommand)]
enum SubCommands {
    Kafka(KafkaCommand),
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
/// start kafka backed proximo instance.
#[argh(subcommand, name = "kafka")]
struct KafkaCommand {
    #[argh(positional)]
    // Kafka ervers to connect to.
    servers: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: ReedArgs = argh::from_env();

    let addr = "0.0.0.0:6868".parse().unwrap();

    println!("Proximo Server listening on: {}", addr);

    let source_svc = MessageSourceServer::new(MessageSourceService { args: args.clone() });
    let sink_svc = MessageSinkServer::new(MessageSinkService { args });

    Server::builder()
        .add_service(source_svc)
        .add_service(sink_svc)
        .serve(addr)
        .await?;

    Ok(())
}
