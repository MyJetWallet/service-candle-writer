use chrono::{Datelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{CandleModel, CandleType};

pub static CANDLE_HISTORY_TABLE_NAME: &str = "candlehistory";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleModelEntity {
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "RowKey")]
    pub row_key: String,

    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub datetime_sec: u64,
    pub instrument: String,
}

impl CandleModelEntity {
    pub fn to_entity(
        instrument: &str,
        is_bid: bool,
        candle_type: CandleType,
        candle: CandleModel,
    ) -> Self {
        Self {
            partition_key: Self::get_partition_key(instrument, is_bid, candle_type, candle.datetime),
            row_key: candle.datetime.to_string(),
            close: candle.close,
            high: candle.high,
            low: candle.low,
            open: candle.open,
            datetime_sec: candle.datetime,
            instrument: instrument.to_string(),
        }
    }

    pub fn get_partition_key(
        instrument: &str,
        is_bid: bool,
        candle_type: CandleType,
        datetime: u64,
    ) -> String {
        let bid_ask = if is_bid { "bid" } else { "ask" };
        let date = Utc.timestamp_millis_opt((datetime * 1000) as i64).unwrap();
        let partition = match candle_type {
            CandleType::Minute => format!("min-{}-{}", date.year(), date.month()),
            CandleType::Hour => format!("hour-{}-{}", date.year(), date.month()),
            CandleType::Day =>format!("day-{}", date.year()),
            CandleType::Month => "month".to_string(),
        };

        format!("{}-{}-{}", instrument, bid_ask, partition)
    }
}
