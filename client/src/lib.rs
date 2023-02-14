use service_candle_writer_generated_proto::rust_grpc_service::bookstore_client::BookstoreClient;
use tonic;
pub struct ExampleClientBuilder{}

impl ExampleClientBuilder {
    pub async fn new(url: String) -> BookstoreClient<tonic::transport::Channel> {
        BookstoreClient::connect::<_>(
            url.clone(),
        ).await.unwrap()
    }
}
