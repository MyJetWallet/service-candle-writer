use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use azure_core::Pageable;
use azure_data_tables::{
    operations::QueryEntityResponse,
    prelude::{TableClient, TableServiceClient},
};
use chrono::{Days, Duration, Months, TimeZone, Utc};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::{
    app::AppContext,
    models::{CandleModel, CandleModelEntity, CandleType},
};

use super::get_table_name;

#[async_trait]
pub trait Database<T> {
    async fn read(&self) -> T;
    async fn increase(&self);
}

pub async fn persist_candles(context: &Arc<AppContext>, latest_timestamp: u64, current_time: u64) {
    let candle_types = [
        CandleType::Minute,
        CandleType::Hour,
        CandleType::Day,
        CandleType::Month,
    ];
    let mut persist_ask = Vec::with_capacity(100);
    let mut persist_bid = Vec::with_capacity(100);
    context.instrument_storage.persist().await;

    //todo: remove this
    return;
    {
        let guard = context.cache.ask_candles.read().await;

        for (instrument, candle_cache) in guard.iter() {
            for candle_type in candle_types {
                let to_persist_ask =
                    candle_cache.get_by_date_range(candle_type, latest_timestamp, current_time);

                tracing::info!(
                    "Persist candles for instrument {}, amount: {}",
                    instrument,
                    to_persist_ask.len()
                );

                persist_ask.push((instrument.clone(), candle_type, to_persist_ask));
            }
        }
    }

    {
        let guard = context.cache.bid_candles.read().await;

        for (instrument, candle_cache) in guard.iter() {
            for candle_type in candle_types {
                let to_persist_bid =
                    candle_cache.get_by_date_range(candle_type, latest_timestamp, current_time);

                tracing::info!(
                    "Persist candles for instrument {}, amount: {}",
                    instrument,
                    to_persist_bid.len()
                );

                persist_bid.push((instrument.clone(), candle_type, to_persist_bid));
            }
        }
    }

    for ask in persist_ask {
        let instrument = ask.0;
        let candle_type = ask.1;
        let candles = ask.2;

        let _ = context
            .candles_persistent_azure_storage
            .bulk_save(&instrument, false, candle_type, candles)
            .await;
    }

    for ask in persist_bid {
        let instrument = ask.0;
        let candle_type = ask.1;
        let candles = ask.2;
        let _ = context
            .candles_persistent_azure_storage
            .bulk_save(&instrument, true, candle_type, candles)
            .await;
    }
}

pub async fn restore_candles(context: &Arc<AppContext>) {
    let minute_limit = context.settings.inner.minute_limit as i64;
    let hour_limit = context.settings.inner.hour_limit as i64;
    let current_time = chrono::Utc::now();
    let is_bid_ask = [false, true];
    let candle_types = [
        (
            CandleType::Minute,
            (current_time - Duration::minutes(minute_limit)).timestamp() as u64,
        ),
        (
            CandleType::Hour,
            (current_time - Duration::hours(hour_limit)).timestamp() as u64,
        ),
        (CandleType::Day, u64::MAX),
        (CandleType::Month, u64::MAX),
    ];

    let instruments = context.instrument_storage.instruments.read().await;

    tracing::info!("Restoring candles for {} instruments", instruments.len());

    for instrument in instruments.iter() {
        let instrument = instrument.clone();
        for is_bid in is_bid_ask {
            for (candle_type, limit) in candle_types {
                let mut count = 0;
                let dbg_str = format!(
                    "Working with instrument: {}, is:bid: {}, candle_type: {}",
                    instrument, is_bid, candle_type as i32
                );
                tracing::info!(dbg_str);

                let candles = context
                    .candles_persistent_azure_storage
                    .get_async(&instrument, is_bid, limit, candle_type)
                    .await;

                for candle in candles {
                    context
                        .cache
                        .init(instrument.clone(), is_bid, candle_type, candle)
                        .await;

                    count += 1;
                }

                tracing::info!("{}; Processed: {}", dbg_str, count);
            }
        }
    }
}

pub struct CandlesPersistentAzureStorage {
    table_service_ask: Arc<TableServiceClient>,
    table_service_bid: Arc<TableServiceClient>,
    cloud_tables_bids: Arc<RwLock<HashMap<String, Arc<TableClient>>>>,
    cloud_tables_asks: Arc<RwLock<HashMap<String, Arc<TableClient>>>>,
}

impl CandlesPersistentAzureStorage {
    pub fn new(
        table_service_ask: Arc<TableServiceClient>,
        table_service_bid: Arc<TableServiceClient>,
    ) -> Self {
        Self {
            table_service_ask,
            table_service_bid,
            cloud_tables_bids: Arc::new(RwLock::new(HashMap::new())),
            cloud_tables_asks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_azure_table_storage(
        &self,
        instrument: &str,
        bid: bool,
        candle_type: CandleType,
    ) -> Arc<TableClient> {
        let table_name = get_table_name(candle_type, instrument);

        tracing::info!("Table name: {}", table_name);

        let account = if bid {
            self.table_service_bid.clone()
        } else {
            self.table_service_ask.clone()
        };

        tracing::info!("Bid: {}", bid);

        let cloud_tables = if bid {
            self.cloud_tables_bids.clone()
        } else {
            self.cloud_tables_asks.clone()
        };

        tracing::info!("got cloud tables");

        {
            let table_storage = cloud_tables.read().await;
            let table_storage = table_storage.get(&table_name);

            if let Some(table) = table_storage {
                return table.clone();
            }
        }

        let table_storage = Arc::new(account.table_client(&table_name));
        let _ = table_storage.create().await;
        let return_val = table_storage.clone();
        cloud_tables.write().await.insert(table_name, table_storage);

        return return_val;
    }

    pub async fn bulk_save(
        &self,
        instrument: &str,
        bid: bool,
        candle_type: CandleType,
        candles: Vec<CandleModel>,
    ) {
        let table_storage = self
            .get_azure_table_storage(instrument, bid, candle_type)
            .await;
        let mut entities_by_partition_rows_dict: HashMap<
            String,
            HashMap<String, CandleModelEntity>,
        > = HashMap::new();

        for candle in candles {
            let partition_key =
                CandleModelEntity::generate_partition_key(candle.datetime, candle_type);
            let row_key = CandleModelEntity::generate_row_key(candle.datetime, candle_type);

            if !entities_by_partition_rows_dict.contains_key(&partition_key) {
                let mut map: HashMap<String, CandleModelEntity> = HashMap::new();
                let _ = entities_by_partition_rows_dict
                    .get_mut(&partition_key)
                    .insert(&mut map);
            }

            // get row from Dict, otherwise get it from DB
            let entity = if entities_by_partition_rows_dict
                .get(&partition_key)
                .unwrap()
                .contains_key(&row_key)
            {
                entities_by_partition_rows_dict
                    .get_mut(&partition_key)
                    .unwrap()
                    .get_mut(&row_key)
                    .unwrap()
            } else {
                let get = table_storage
                    .partition_key_client(&partition_key)
                    .entity_client(&row_key)
                    .unwrap()
                    .get()
                    .await;

                let entity = match get {
                    Ok(ent) => ent.entity,
                    Err(_) => CandleModelEntity::create(candle_type, candle.clone()),
                };

                let entry = entities_by_partition_rows_dict
                    .get_mut(&partition_key)
                    .unwrap()
                    .entry(row_key);

                let val = match entry {
                    Entry::Occupied(o) => o.into_mut(),
                    Entry::Vacant(v) => v.insert(entity),
                };

                val
            };

            let mut candles_dict = entity.get_candles(candle_type);

            match candles_dict.entry(candle.datetime) {
                std::collections::btree_map::Entry::Vacant(_) => {
                    candles_dict.insert(candle.datetime, candle);
                }
                std::collections::btree_map::Entry::Occupied(mut val) => {
                    let val = val.get_mut();
                    val.open = candle.open;
                    val.close = candle.close;
                    val.high = candle.high;
                    val.low = candle.low;
                }
            }

            CandleModelEntity::set_candles(entity, candles_dict, 0, candle_type);
        }

        for (partition_key, values) in entities_by_partition_rows_dict.into_iter() {
            //let values =
            let mut transaction_builder = table_storage
                .partition_key_client(&partition_key)
                .transaction();

            for (row_key, entity) in values.into_iter() {
                transaction_builder = transaction_builder
                    .insert_or_replace(row_key, entity, azure_data_tables::IfMatchCondition::Any)
                    .unwrap();
            }

            let res = transaction_builder.into_future().await;

            match res {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("Error while saving candles to Azure; Err: {:?}", err);
                }
            }
        }
        // bulk update is allowed only whithin the same partition
    }

    pub async fn get_async(
        &self,
        instrument: &str,
        bid: bool,
        expiration_date: u64,
        candle_type: CandleType,
    ) -> Vec<CandleModel> {
        let mut result = Vec::with_capacity(2048);
        if candle_type == CandleType::Day || candle_type == CandleType::Month {
            let table_storage = self
                .get_azure_table_storage(instrument, bid, candle_type)
                .await;

            // for these types simply iterate through all records
            let mut stream: Pageable<QueryEntityResponse<CandleModelEntity>, _> =
                table_storage.query().into_stream();
            while let Some(entity) = stream.next().await {
                let entity = entity.unwrap();

                for candle in entity.entities {
                    let candles = candle.get_candles(candle_type);

                    for candle in candles.into_iter() {
                        result.push(candle.1);
                    }
                }
            }
        } else {
            // prepare list for getting data by partitons
            let mut partitions_key_list = vec![CandleModelEntity::generate_partition_key(
                expiration_date,
                candle_type,
            )];

            let mut next_partition_date = expiration_date;

            'outer: loop {
                let date_time = Utc
                    .timestamp_millis_opt((next_partition_date * 1000) as i64)
                    .unwrap();
                // fill up list with partition keys based on Key granularity
                match candle_type {
                    CandleType::Minute => {
                        next_partition_date =
                            (date_time.checked_add_days(Days::new(1)).unwrap()).timestamp() as u64;
                    }
                    CandleType::Hour => {
                        next_partition_date =
                            (date_time.checked_add_months(Months::new(1)).unwrap()).timestamp()
                                as u64;
                    }
                    CandleType::Day => todo!(),
                    CandleType::Month => todo!(),
                };

                partitions_key_list.push(CandleModelEntity::generate_partition_key(
                    next_partition_date,
                    candle_type,
                ));
                let current_time = chrono::Utc::now().timestamp() as u64;
                if next_partition_date > current_time {
                    break 'outer;
                }
            }

            let table_storage = self
                .get_azure_table_storage(instrument, bid, candle_type)
                .await;

            tracing::info!("Got table storage!");

            // iterate by partitions
            for partition_key in partitions_key_list {
                tracing::info!("Getting partition: {}", partition_key);
                let mut stream: Pageable<QueryEntityResponse<CandleModelEntity>, _> = table_storage
                    .query()
                    .initial_partition_key(&partition_key)
                    .into_stream();

                while let Some(entity) = stream.next().await {
                    if let Ok(entity) = entity {
                        tracing::info!("Got entity: {:?}", entity);
                        for candle in entity.entities {
                            let candles = candle.get_candles(candle_type);
                            for candle in candles.into_iter() {
                                tracing::info!("Got candle: {:?}", candle.1);
                                result.push(candle.1);
                            }
                        }
                    }
                }
            }
        }

        result
    }
}
