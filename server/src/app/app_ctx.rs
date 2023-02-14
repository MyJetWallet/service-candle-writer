use std::sync::Arc;

use crate::{
    caches::CandlesInstrumentsCache,
    domain::{Database, DatabaseImpl, InstrumentStorage, RequestCounter},
    settings_model::SettingsModel,
    subscribers::BidAskSubscriber,
};
use azure_data_tables::prelude::TableServiceClient;
use azure_storage::StorageCredentials;
use my_no_sql_tcp_reader::MyNoSqlTcpConnectionSettings;
use my_service_bus_tcp_client::{MyServiceBusClient, MyServiceBusSettings};

pub struct AppContext {
    pub states: rust_service_sdk::app::global_states::GlobalStates,
    pub database: Arc<dyn Database<RequestCounter> + Sync + Send>,
    pub service_bus: Arc<MyServiceBusClient>,
    pub table_service: Arc<TableServiceClient>,
    pub cache: Arc<CandlesInstrumentsCache>,
    pub instrument_storage: Arc<InstrumentStorage>,
    pub settings: SettingsModel,
    //_my_no_sql_tcp_connection: my_no_sql_tcp_reader::MyNoSqlTcpConnection,
}

struct RealMyServiceBusSettings {
    host_port: String,
}

struct RealMyNoSqlTcpConnectionSettings {
    host_port: String,
}

#[async_trait::async_trait]
impl MyServiceBusSettings for RealMyServiceBusSettings {
    async fn get_host_port(&self) -> String {
        self.host_port.clone()
    }
}

#[async_trait::async_trait]
impl MyNoSqlTcpConnectionSettings for RealMyNoSqlTcpConnectionSettings {
    async fn get_host_port(&self) -> String {
        self.host_port.clone()
    }
}

impl AppContext {
    pub async fn new(settings: SettingsModel) -> Self {
        let service_bus_settings = RealMyServiceBusSettings {
            host_port: settings.inner.spot_service_bus_hos_port.clone(),
        };
        let _no_sql_settings = RealMyNoSqlTcpConnectionSettings {
            host_port: settings.inner.my_no_sql_reader_host_port.clone(),
        };

        let logger = Arc::new(rust_service_sdk::adapters::LoggerAdapter {});
        let service_bus = Arc::new(MyServiceBusClient::new(
            "service-candle-writer",
            &settings.inner.spot_service_bus_hos_port,
            Arc::new(service_bus_settings),
            logger,
        ));

        let cache = Arc::new(CandlesInstrumentsCache::new(
            settings.inner.minute_limit,
            settings.inner.hour_limit,
        ));

        let storage_credentials = StorageCredentials::Key(
            settings.inner.azure_storage_account.clone(),
            settings.inner.azure_storage_access_key.clone(),
        );
        let table_service = TableServiceClient::new(
            settings.inner.azure_storage_account.clone(),
            storage_credentials,
        );

        let table_client = table_service;

        let table_service = Arc::new(table_client);

        let instrument_storage = Arc::new(InstrumentStorage::new(table_service.clone()));

        let subscriber = BidAskSubscriber::new(
            cache.clone(),
            service_bus.clone(),
            instrument_storage.clone(),
        );

        service_bus
            .subscribe(
                "service-candle-writer".to_string(),
                my_service_bus_abstractions::subscriber::TopicQueueType::Permanent,
                Arc::new(subscriber),
            )
            .await;

        Self {
            states: rust_service_sdk::app::global_states::GlobalStates::new(),
            database: Arc::new(DatabaseImpl::new()),
            service_bus,
            table_service,
            cache,
            instrument_storage,
            settings: settings,
        }
    }
}

impl rust_service_sdk::app::app_ctx::GetGlobalState for AppContext {
    fn is_initialized(&self) -> bool {
        self.states.is_initialized()
    }

    fn is_shutting_down(&self) -> bool {
        self.states.is_shutting_down()
    }

    fn shutting_down(&self) {
        self.states
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl rust_service_sdk::app::app_ctx::InitGrpc for AppContext {
    fn init_grpc(
        &self,
        server: Box<std::cell::RefCell<tonic::transport::Server>>,
    ) -> tonic::transport::server::Router {
        let bookstore = crate::services::BookStoreImpl::new(self.database.clone());

        server.borrow_mut().add_service(
            service_candle_writer_generated_proto::bookstore_server::BookstoreServer::new(
                bookstore,
            ),
        )
    }
}
