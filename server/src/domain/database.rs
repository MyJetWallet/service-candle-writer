use std::sync::Arc;

use async_trait::async_trait;
use azure_core::Pageable;
use azure_data_tables::operations::QueryEntityResponse;
use futures::StreamExt;

use crate::{
    app::AppContext,
    models::{CandleModel, CandleModelEntity, CandleType},
};

#[async_trait]
pub trait Database<T> {
    async fn read(&self) -> T;
    async fn increase(&self);
}

pub async fn persist_candles(context: &Arc<AppContext>, latest_timestamp: u64, current_time: u64) {
    let table_client = context
        .table_service
        .table_client(crate::models::CANDLE_HISTORY_TABLE_NAME);
    let candle_types = [
        CandleType::Minute,
        CandleType::Hour,
        CandleType::Day,
        CandleType::Month,
    ];
    let mut persist_ask = Vec::with_capacity(100);
    let mut persist_bid = Vec::with_capacity(100);
    context.instrument_storage.persist().await;
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
        for candle in candles {
            let entity = CandleModelEntity::to_entity(&instrument, false, candle_type, candle);
            let entity_client = table_client
                .partition_key_client(&entity.partition_key)
                .entity_client(&entity.row_key)
                .unwrap();

            let res = entity_client.insert_or_replace(&entity).unwrap().await;

            match res {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(
                        "Error while persisting candle: {:?}; ENTITY: {:?}",
                        err,
                        entity
                    )
                }
            }
        }
    }

    for ask in persist_bid {
        let instrument = ask.0;
        let candle_type = ask.1;
        let candles = ask.2;
        for candle in candles {
            let entity = CandleModelEntity::to_entity(&instrument, true, candle_type, candle);
            let entity_client = table_client
                .partition_key_client(&entity.partition_key)
                .entity_client(&entity.row_key)
                .unwrap();

            let res = entity_client.insert_or_replace(&entity).unwrap().await;

            match res {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(
                        "Error while persisting candle: {:?}; ENTITY: {:?}",
                        err,
                        entity
                    )
                }
            }
        }
    }
}

pub async fn restore_candles(context: &Arc<AppContext>) {
    let minute_limit: usize = context.settings.inner.minute_limit;
    let hour_limit: usize = context.settings.inner.hour_limit;
    let current_time = chrono::Utc::now().timestamp() as u64;
    let is_bid_ask = [false, true];
    let candle_types = [
        (CandleType::Minute, minute_limit),
        (CandleType::Hour, hour_limit),
        (CandleType::Day, usize::MAX),
        (CandleType::Month, usize::MAX),
    ];

    let table_client = context
        .table_service
        .table_client(crate::models::CANDLE_HISTORY_TABLE_NAME);
    let instruments = context.instrument_storage.instruments.read().await;

    tracing::info!("Restoring candles for {} instruments", instruments.len());

    for instrument in instruments.iter() {
        let instrument = instrument.clone();
        for is_bid in is_bid_ask {
            for (candle_type, limit) in candle_types {
                let mut count = 0;
                let partition_key = CandleModelEntity::get_partition_key(
                    &instrument,
                    is_bid,
                    candle_type,
                    current_time,
                );

                tracing::info!("Working with partition {}", partition_key);

                let mut stream: Pageable<QueryEntityResponse<CandleModelEntity>, _> = table_client
                    .query()
                    .initial_partition_key(&partition_key)
                    .into_stream();

                'outer: loop {
                    if let Some(item) = stream.next().await {
                        match item {
                            Ok(entity) => {
                                for candle_model in entity.entities {
                                    if count > limit {
                                        break 'outer;
                                    }
                                    count += 1;
                                    let candle = CandleModel {
                                        open: candle_model.open,
                                        close: candle_model.close,
                                        high: candle_model.high,
                                        low: candle_model.low,
                                        datetime: candle_model.datetime_sec,
                                    };

                                    context
                                        .cache
                                        .init(instrument.clone(), is_bid, candle_type, candle)
                                        .await;

                                    tracing::info!(
                                        "Working with partition {}; Processed: {}",
                                        partition_key,
                                        count
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    "Error while restoring instrument's storage: {:?};",
                                    err
                                )
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
