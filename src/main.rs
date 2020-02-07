use argh::FromArgs;
use futures::StreamExt;
use log::info;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use proximo::message_sink_server::{MessageSink, MessageSinkServer};
use proximo::message_source_server::{MessageSource, MessageSourceServer};
use proximo::{Confirmation, ConsumerRequest, Message, Offset, PublisherRequest};

mod kafka;
mod mem;

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
            match i {
                Ok(ConsumerRequest {
                    confirmation: Some(c),
                    ..
                }) => Some(Ok(c)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
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
            SubCommands::Mem(_) => {
                mem::consume(
                    &start.topic,
                    &start.consumer,
                    Offset::from_i32(start.initial_offset).unwrap(),
                    ack_tx,
                    Box::pin(stream),
                )
                .await;
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
            match i {
                Ok(PublisherRequest { msg: Some(m), .. }) => Some(Ok(m)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        });

        match &self.args.subcommand {
            SubCommands::Kafka(command) => {
                kafka::publish(&command.servers, &start.topic, ack_tx, Box::pin(stream));
            }
            SubCommands::Mem(_) => {
                mem::publish(&start.topic, ack_tx, Box::pin(stream)).await;
            }
        }

        Ok(Response::new(ack_rx))
    }
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
/// Reed: Proximo Server implementation written in Rust
struct ReedArgs {
    #[argh(subcommand)]
    subcommand: SubCommands,

    #[argh(option, default = "6868")]
    /// port to use
    port: usize,
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
#[argh(subcommand)]
enum SubCommands {
    Kafka(KafkaCommand),
    Mem(MemCommand),
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
/// start kafka backed proximo instance
#[argh(subcommand, name = "kafka")]
struct KafkaCommand {
    #[argh(positional)]
    /// kafka servers to connect to
    servers: String,
}

#[derive(FromArgs, PartialEq, Debug, Clone)]
/// start mem backed proximo instance
#[argh(subcommand, name = "mem")]
struct MemCommand {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: ReedArgs = argh::from_env();

    env_logger::init();

    let addr = format!("0.0.0.0:{}", args.port).parse().unwrap();

    info!("Proximo Server listening on: {}", addr);

    let source_svc = MessageSourceServer::new(MessageSourceService { args: args.clone() });
    let sink_svc = MessageSinkServer::new(MessageSinkService { args });

    Server::builder()
        .add_service(source_svc)
        .add_service(sink_svc)
        .serve(addr)
        .await?;

    Ok(())
}
