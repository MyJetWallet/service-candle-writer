use crate::models::{CandleModel, CandleType};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum CacheType {
    UnLimited,
    Limited(usize),
}

#[derive(Debug, Clone)]
pub struct CandlesCache {
    pub candle_type: CandleType,
    pub candles: BTreeMap<u64, CandleModel>,
    cache_type: CacheType,
}

impl CandlesCache {
    pub fn new(candle_type: CandleType) -> Self {
        Self {
            candle_type: candle_type,
            candles: BTreeMap::new(),
            cache_type: CacheType::UnLimited,
        }
    }

    pub fn with_capacity(candle_type: CandleType, capacity: usize) -> Self {
        Self {
            candle_type: candle_type,
            candles: BTreeMap::new(),
            cache_type: CacheType::Limited(capacity),
        }
    }

    pub fn init(&mut self, candle: CandleModel) {
        self.candles.insert(candle.datetime, candle);
    }

    pub fn handle_new_rate(&mut self, date: u64, rate: f64) -> (CandleType, CandleModel) {
        let date = self.candle_type.format_date_by_type(date);

        let target_candle = self.candles.get_mut(&date);

        match target_candle {
            Some(candle) => {
                candle.update_by_rate(rate);
                (self.candle_type, candle.clone())
            }
            None => {
                let candle_model = CandleModel::new_from_rate(self.candle_type.clone(), date, rate);

                // Cache resizing
                if let CacheType::Limited(capacity) = self.cache_type {
                    if self.candles.len() >= capacity {
                        let key_to_remove = *self.candles.keys().next().unwrap();
                        self.candles.remove(&key_to_remove);
                    }
                }

                let response = candle_model.clone();
                self.candles.insert(date, candle_model);
                (self.candle_type, response)
            }
        }
    }

    pub fn get_by_date_range(&self, date_from: u64, date_to: u64) -> Vec<CandleModel> {
        let mut result = Vec::new();

        for (_, candle) in self.candles.range(date_from..date_to) {
            result.push(candle.clone());
        }

        result
    }

    pub fn clear(&mut self) {
        self.candles.clear()
    }
}
