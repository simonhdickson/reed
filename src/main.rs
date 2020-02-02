use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use proximo::message_source_server::{MessageSource, MessageSourceServer};
use proximo::{ConsumerRequest, Message};

mod kafka;

mod proximo {
    tonic::include_proto!("proximo");
}

#[derive(Debug)]
pub struct MessageSourceService {}

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

        kafka::start(&start.topic, &start.consumer, ack_tx, Box::pin(stream));

        Ok(Response::new(ack_rx))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:6868".parse().unwrap();

    println!("Proximo Server listening on: {}", addr);

    let message_source = MessageSourceService {};

    let svc = MessageSourceServer::new(message_source);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
