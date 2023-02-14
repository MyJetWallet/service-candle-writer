use crate::models::{CandleModel, CandleType};

use super::CandlesCache;

#[derive(Debug, Clone)]
pub struct CandleTypeCache {
    pub instrument_id: String,
    pub candles_by_minute: CandlesCache,
    pub candles_by_hour: CandlesCache,
    pub candles_by_day: CandlesCache,
    pub candles_by_month: CandlesCache,
}

impl CandleTypeCache {
    pub fn new(instrument_id: String, minute_capacity: usize, hour_capacity: usize) -> Self {
        Self {
            instrument_id: instrument_id,
            candles_by_minute: CandlesCache::with_capacity(CandleType::Minute, minute_capacity),
            candles_by_hour: CandlesCache::with_capacity(CandleType::Hour, hour_capacity),
            candles_by_day: CandlesCache::new(CandleType::Day),
            candles_by_month: CandlesCache::new(CandleType::Month),
        }
    }

    pub fn init(&mut self, candle: CandleModel, candle_type: CandleType) {
        match candle_type {
            CandleType::Minute => self.candles_by_minute.init(candle),
            CandleType::Hour => self.candles_by_hour.init(candle),
            CandleType::Day => self.candles_by_day.init(candle),
            CandleType::Month => self.candles_by_month.init(candle),
        };
    }

    pub fn get_by_date_range(
        &self,
        candle_type: CandleType,
        date_from: u64,
        date_to: u64,
    ) -> Vec<CandleModel> {
        match candle_type {
            CandleType::Minute => self.candles_by_minute.get_by_date_range(date_from, date_to),
            CandleType::Hour => self.candles_by_hour.get_by_date_range(date_from, date_to),
            CandleType::Day => self.candles_by_day.get_by_date_range(date_from, date_to),
            CandleType::Month => self.candles_by_month.get_by_date_range(date_from, date_to),
        }
    }

    pub fn handle_new_rate(&mut self, rate: f64, date: u64) -> Vec<(CandleType, CandleModel)> {
        vec![
            self.candles_by_minute.handle_new_rate(date, rate),
            self.candles_by_hour.handle_new_rate(date, rate),
            self.candles_by_day.handle_new_rate(date, rate),
            self.candles_by_month.handle_new_rate(date, rate),
        ]
    }

    pub fn clear(&mut self) {
        self.candles_by_day.clear();
        self.candles_by_hour.clear();
        self.candles_by_minute.clear();
        self.candles_by_month.clear();
    }
}
