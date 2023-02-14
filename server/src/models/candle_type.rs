use chrono::{DateTime, Datelike, Utc};
use chrono::{TimeZone};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize_repr, Deserialize_repr, Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive, Hash, Eq, PartialEq)]
#[repr(i32)]
pub enum CandleType {
    Minute = 0,
    Hour = 1,
    Day = 2,
    Month = 3,
}

impl CandleType {
    pub fn format_date_by_type(&self, date: u64) -> u64 {
        match self {
            CandleType::Minute => date - date % 60,
            CandleType::Hour => date - date % 3600,
            CandleType::Day => date - date % 86400,
            CandleType::Month => {
                let date = Utc.timestamp_millis_opt((date * 1000) as i64).unwrap();
                let start_of_month: DateTime<Utc> = Utc
                    .with_ymd_and_hms(date.year(), date.month(), 1, 0, 0, 0)
                    .unwrap();

                return start_of_month.timestamp() as u64;
            }
        }
    }

    pub fn candle_timestamp_sec(&self, timestamp_sec: i64) -> i64 {
        match self {
            CandleType::Minute => timestamp_sec - timestamp_sec % 60,
            CandleType::Hour => timestamp_sec - timestamp_sec % 3600,
            CandleType::Day => timestamp_sec - timestamp_sec % 86400,
            CandleType::Month => {
                let date = Utc.timestamp_millis_opt(timestamp_sec * 1000).unwrap();
                let start_of_month: DateTime<Utc> = Utc
                    .with_ymd_and_hms(date.year(), date.month(), 1, 0, 0, 0)
                    .unwrap();

                return start_of_month.timestamp();
            }
        }
    }
}
