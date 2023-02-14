use std::{collections::HashSet, sync::{Arc, atomic::AtomicBool}};

use futures::stream::StreamExt;
use azure_core::Pageable;
use azure_data_tables::{prelude::{TableClient, TableServiceClient}, operations::QueryEntityResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

pub static TABLE_NAME: &str = "instrumentstorage";
pub static PARTITION_KEY: &str = "INSTRUMENTSTORAGE";

pub struct InstrumentStorage {
    pub instruments: RwLock<HashSet<String>>,
    pub persist_table_client: Arc<TableClient>,
    is_table_created: AtomicBool,
    persist_queue: Mutex<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentStorageEntity {
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "RowKey")]
    pub instrument: String,
}

impl InstrumentStorage {
    pub fn new(table_service_client: Arc<TableServiceClient>) -> Self {
        Self {
            instruments: RwLock::new(HashSet::new()),
            persist_table_client: Arc::new(table_service_client.table_client(TABLE_NAME)),
            is_table_created: AtomicBool::new(false),
            persist_queue: Mutex::new(Vec::with_capacity(100)),
        }
    }

    pub async fn add(&self, instrument: String) {
        let mut set = self.instruments.write().await;
        if set.contains(&instrument) {
            return;
        }

        set.insert(instrument.clone());
        self.persist_queue.lock().await.push(instrument);
    }

    pub async fn contains(&self, instrument: &str) -> bool {
        self.instruments.read().await.contains(instrument)
    }

    pub async fn persist(&self) {
        let table_client = self.persist_table_client.clone();

        if !self.is_table_created.load(std::sync::atomic::Ordering::Acquire) {
            let _ = table_client.create().await;
            self.is_table_created.store(true, std::sync::atomic::Ordering::Release);
        }

        let instruments = &mut self.persist_queue.lock().await;

        if instruments.is_empty() {
            return;
        }

        while let Some(instrument) = instruments.pop() {
            let entity_client = table_client
                .partition_key_client(PARTITION_KEY)
                .entity_client(&instrument)
                .unwrap();

            let entity = InstrumentStorageEntity {
                instrument: instrument,
                partition_key: PARTITION_KEY.to_string(),
            };

            let res = entity_client.insert_or_replace(entity).unwrap().await;

            match res {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error while persisting instrument: {:?};", err)
                }
            }
        }
    }

    pub async fn restore(&self) {
        let table_client = self.persist_table_client.clone();
        
        if !self.is_table_created.load(std::sync::atomic::Ordering::Acquire) {
            let _ = table_client.create().await;
            self.is_table_created.store(true, std::sync::atomic::Ordering::Release);
        }

        tracing::info!("Restoring instrument's storage...");

        let mut count = 0;
        let mut stream: Pageable<QueryEntityResponse<InstrumentStorageEntity>, _> = table_client
            .query()
            .initial_partition_key(PARTITION_KEY)
            .into_stream();

            while let Some(item) = stream.next().await {
                match item {
                    Ok(entity) => {
                        let mut set = self.instruments.write().await;
                        for entity in entity.entities {
                            count += 1;
                            set.insert(entity.instrument);
                        }
                    }
                    Err(err) => {
                        tracing::error!("Error while restoring instrument's storage: {:?};", err)
                    }
                }
            }

            tracing::info!("Restored instrument's storage; count: {}", count);
    }
}
