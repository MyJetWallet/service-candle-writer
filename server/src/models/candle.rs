use serde::{Deserialize, Serialize};

use super::CandleType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleModel {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub datetime: u64,
}

impl CandleModel {
    pub fn new_from_rate(candle_type: CandleType, date: u64, rate: f64) -> Self {
        let date = candle_type.format_date_by_type(date);

        Self {
            open: rate,
            close: rate,
            high: rate,
            low: rate,
            datetime: date,
        }
    }

    pub fn update_by_rate(&mut self, rate: f64) {
        self.close = rate;

        if self.high < rate {
            self.high = rate;
        }

        if self.low > rate {
            self.low = rate;
        }
    }
}
