use crate::models::{CandleModel, CandleType, CandlesBidAsk};
use std::collections::HashMap;
use tokio::sync::RwLock;

use super::CandleTypeCache;

pub struct CandlesInstrumentsCache {
    pub bid_candles: RwLock<HashMap<String, CandleTypeCache>>,
    pub ask_candles: RwLock<HashMap<String, CandleTypeCache>>,
    minute_capacity: usize,
    hour_capacity: usize,
}

impl CandlesInstrumentsCache {
    pub fn new(minute_capacity: usize, hour_capacity: usize) -> Self {
        Self {
            bid_candles: RwLock::new(HashMap::new()),
            ask_candles: RwLock::new(HashMap::new()),
            minute_capacity,
            hour_capacity,
        }
    }

    pub async fn update(
        &self,
        prices: Vec<CandlesBidAsk>,
    ) -> (
        Vec<(CandleType, CandleModel)>,
        Vec<(CandleType, CandleModel)>,
    ) {
        (
            self.update_bid_or_ask(true, &prices).await,
            self.update_bid_or_ask(false, &prices).await,
        )
    }

    async fn update_bid_or_ask(
        &self,
        is_bid: bool,
        prices: &Vec<CandlesBidAsk>,
    ) -> Vec<(CandleType, CandleModel)> {
        let mut write_lock = match is_bid {
            true => self.bid_candles.write().await,
            false => self.ask_candles.write().await,
        };

        let mut result = Vec::with_capacity(prices.len() * 4);
        for bid_ask in prices.iter() {
            let target_instruments_cache = write_lock.get_mut(&bid_ask.instrument);
            let target_rate = match is_bid {
                true => bid_ask.bid,
                false => bid_ask.ask,
            };

            let candle_updates;
            match target_instruments_cache {
                Some(cache) => {
                    candle_updates = cache.handle_new_rate(target_rate, bid_ask.date);
                }
                None => {
                    let mut cache = CandleTypeCache::new(
                        bid_ask.instrument.clone(),
                        self.minute_capacity,
                        self.hour_capacity,
                    );
                    candle_updates = cache.handle_new_rate(target_rate, bid_ask.date);
                    write_lock.insert(bid_ask.instrument.clone(), cache);
                }
            }

            result.extend(candle_updates);
        }

        result
    }

    pub async fn init(
        &self,
        instument_id: String,
        is_bid: bool,
        candle_type: CandleType,
        candle: CandleModel,
    ) {
        let mut target_cache = match is_bid {
            true => self.bid_candles.write().await,
            false => self.ask_candles.write().await,
        };

        let instrument_cache = target_cache.get_mut(&instument_id);

        match instrument_cache {
            Some(cache) => {
                cache.init(candle, candle_type);
            }
            None => {
                let mut cache = CandleTypeCache::new(instument_id.clone(),
                 self.minute_capacity, self.hour_capacity);
                cache.init(candle, candle_type);
                target_cache.insert(instument_id, cache);
            }
        }
    }

    pub async fn get_by_date_range(
        &self,
        instument_id: String,
        candle_type: CandleType,
        is_bid: bool,
        start_date: u64,
        end_date: u64,
    ) -> Vec<CandleModel> {
        let target_cache = match is_bid {
            true => self.bid_candles.read().await,
            false => self.ask_candles.read().await,
        };

        let instrument_cache = target_cache.get(&instument_id);

        match instrument_cache {
            Some(cache) => cache.get_by_date_range(candle_type, start_date, end_date),
            None => {
                vec![]
            }
        }
    }

    pub async fn clear(&mut self) {
        {
            let mut bids = self.bid_candles.write().await;
            bids.clear();
        }
        {
            let mut asks = self.ask_candles.write().await;
            asks.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::CandlesBidAsk;

    use super::CandlesInstrumentsCache;

    #[tokio::test]
    async fn test_sinle_quote() {
        let cache = CandlesInstrumentsCache::new(100, 100);
        let instument = String::from("EURUSD");

        let bid_ask = CandlesBidAsk {
            date: 1662559404,
            instrument: instument.clone(),
            bid: 25.55,
            ask: 36.55,
        };

        cache.update(vec![bid_ask]).await;

        let result_bid_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_hour = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Hour,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_hour = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Hour,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_day = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Day,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_day = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Day,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_mount = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Month,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_mount = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Month,
                false,
                1660559404,
                2660559404,
            )
            .await;

        assert_eq!(result_bid_minute.len(), 1);
        assert_eq!(result_ask_minute.len(), 1);

        assert_eq!(result_bid_hour.len(), 1);
        assert_eq!(result_ask_hour.len(), 1);

        assert_eq!(result_bid_day.len(), 1);
        assert_eq!(result_ask_day.len(), 1);

        assert_eq!(result_bid_mount.len(), 1);
        assert_eq!(result_ask_mount.len(), 1);
    }

    #[tokio::test]
    async fn test_date_rotation_minute() {
        let cache = CandlesInstrumentsCache::new(100, 100);
        let instument = String::from("EURUSD");

        let bid_ask = CandlesBidAsk {
            date: 1662559404,
            instrument: instument.clone(),
            bid: 25.55,
            ask: 36.55,
        };

        cache.update(vec![bid_ask]).await;

        let bid_ask = CandlesBidAsk {
            date: 1662559474,
            instrument: instument.clone(),
            bid: 25.55,
            ask: 36.55,
        };

        cache.update(vec![bid_ask]).await;

        let result_bid_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_hour = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Hour,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_hour = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Hour,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_day = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Day,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_day = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Day,
                false,
                1660559404,
                2660559404,
            )
            .await;

        let result_bid_mount = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Month,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_mount = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Month,
                false,
                1660559404,
                2660559404,
            )
            .await;

        assert_eq!(result_bid_minute.len(), 2);
        assert_eq!(result_ask_minute.len(), 2);

        assert_eq!(result_bid_hour.len(), 1);
        assert_eq!(result_ask_hour.len(), 1);

        assert_eq!(result_bid_day.len(), 1);
        assert_eq!(result_ask_day.len(), 1);

        assert_eq!(result_bid_mount.len(), 1);
        assert_eq!(result_ask_mount.len(), 1);
    }

    #[tokio::test]
    async fn test_calculation() {
        let cache = CandlesInstrumentsCache::new(100, 100);
        let instument = String::from("EURUSD");

        let bid_ask = CandlesBidAsk {
            date: 1662559404,
            instrument: instument.clone(),
            bid: 25.55,
            ask: 36.55,
        };

        cache.update(vec![bid_ask]).await;

        let bid_ask = CandlesBidAsk {
            date: 1662559406,
            instrument: instument.clone(),
            bid: 60.55,
            ask: 31.55,
        };

        cache.update(vec![bid_ask]).await;

        let bid_ask = CandlesBidAsk {
            date: 1662559407,
            instrument: instument.clone(),
            bid: 50.55,
            ask: 62.55,
        };

        cache.update(vec![bid_ask]).await;

        let result_bid_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                false,
                1660559404,
                2660559404,
            )
            .await;

        assert_eq!(result_bid_minute.len(), 1);
        assert_eq!(result_ask_minute.len(), 1);

        let first_bid = result_bid_minute.first().unwrap();
        let first_ask = result_ask_minute.first().unwrap();

        assert_eq!(first_bid.open, 25.55);
        assert_eq!(first_bid.close, 50.55);
        assert_eq!(first_bid.high, 60.55);
        assert_eq!(first_bid.low, 25.55);

        assert_eq!(first_ask.open, 36.55);
        assert_eq!(first_ask.close, 62.55);
        assert_eq!(first_ask.high, 62.55);
        assert_eq!(first_ask.low, 31.55);

        assert_eq!(first_bid.datetime, 1662559380);
        assert_eq!(first_ask.datetime, 1662559380);
    }

    #[tokio::test]
    async fn test_minute_limit() {
        let limit = 100;
        let cache = CandlesInstrumentsCache::new(limit, limit);
        let instument = String::from("EURUSD");

        let mut arr = Vec::with_capacity(limit);

        for i in 0..=(limit + 50) {
            let bid_ask = CandlesBidAsk {
                date: 1662559404 + 60 * i as u64,
                instrument: instument.clone(),
                bid: 25.55 + i as f64,
                ask: 35.55 + i as f64,
            };

            arr.push(bid_ask);
        }

        cache.update(arr).await;

        let result_bid_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                true,
                1660559404,
                2660559404,
            )
            .await;
        let result_ask_minute = cache
            .get_by_date_range(
                instument.clone(),
                crate::models::CandleType::Minute,
                false,
                1660559404,
                2660559404,
            )
            .await;

        assert_eq!(result_bid_minute.len(), limit);
        assert_eq!(result_ask_minute.len(), limit);

        let last_ask = result_ask_minute.last().unwrap();
        let last_bid = result_bid_minute.last().unwrap();

        let add = (limit + 50) as f64;

        println!("last_bid: {:?}", last_bid);
        println!("last_ask: {:?}", last_ask);

        assert_eq!(last_bid.open, 25.55 + add);
        assert_eq!(last_bid.close, 25.55 + add);
        assert_eq!(last_bid.high, 25.55 + add);
        assert_eq!(last_bid.low, 25.55 + add);

        assert_eq!(last_ask.open, 35.55 + add);
        assert_eq!(last_ask.close, 35.55 + add);
        assert_eq!(last_ask.high, 35.55 + add);
        assert_eq!(last_ask.low, 35.55 + add);
    }
}
