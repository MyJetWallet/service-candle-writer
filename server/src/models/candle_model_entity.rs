use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{CandleModel, CandleType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleModelEntity {
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "RowKey")]
    pub row_key: String,

    #[serde(rename = "Data")]
    pub data: String,
}

impl CandleModelEntity {

    pub fn create(candle_type: CandleType, candle: CandleModel) -> Self {
        return Self {
            partition_key : CandleModelEntity::generate_partition_key(candle.datetime, candle_type),
            row_key: CandleModelEntity::generate_row_key(candle.datetime, candle_type),
            data: "".to_string(),
        };
    }

    pub fn get_candles(&self, candle_type: CandleType) -> BTreeMap<u64, CandleModel> {
        return CandleModelEntity::data_string_to_candle_grpc_model(
            &self.data,
            candle_type,
            &self.partition_key,
            &self.row_key,
        );
    }

    pub fn set_candles(
        &mut self,
        items: BTreeMap<u64, CandleModel>,
        _digits: i32,
        candle_type: CandleType,
    ) {
        self.data = CandleModelEntity::to_data_string(items, candle_type);
    }

    pub fn generate_partition_key(date_time: u64, candle_type: CandleType) -> String {
        let dateTime = Utc.timestamp_millis_opt((date_time * 1000) as i64).unwrap();
        return match candle_type {
            CandleType::Minute => format!(
                "{}{}{}",
                dateTime.format("%Y"),
                dateTime.format("%m"),
                dateTime.format("%d"),
            ),
            CandleType::Hour => format!("{}{}", dateTime.format("%Y"), dateTime.format("%m"),),
            CandleType::Day => dateTime.format("%Y").to_string(),
            CandleType::Month => dateTime.format("%Y").to_string(),
        };
    }

    pub fn generate_row_key(date_time: u64, candle_type: CandleType) -> String {
        let date_time = Utc.timestamp_millis_opt((date_time * 1000) as i64).unwrap();
        return match candle_type {
            CandleType::Minute => date_time.format("%H").to_string(),
            CandleType::Hour => date_time.format("%d").to_string(),
            CandleType::Day => date_time.format("%m").to_string(),
            CandleType::Month => date_time.format("%Y").to_string(),
        };
    }

    pub fn to_date_part_string(datetime: u64, candle_type: CandleType) -> String {
        let dt = Utc.timestamp_millis_opt((datetime * 1000) as i64).unwrap();
        return match candle_type {
            CandleType::Month => dt.format("%m").to_string(),
            CandleType::Day => dt.format("%d").to_string(),
            CandleType::Minute => dt.format("%M").to_string(),
            CandleType::Hour => dt.format("%H").to_string(),
        };
    }

    pub fn to_data_string(items: BTreeMap<u64, CandleModel>, candle_type: CandleType) -> String {
        let mut result = String::with_capacity(20);
        for (datetime, candle) in items.into_iter() {
            if result.len() > 0 {
                result.push('|');
            }

            let mut concat = String::with_capacity(20);

            concat.push_str(&CandleModelEntity::to_date_part_string(
                datetime,
                candle_type,
            ));
            concat.push(';');
            concat.push_str(&candle.open.to_string());
            concat.push(';');
            concat.push_str(&candle.close.to_string());
            concat.push(';');
            concat.push_str(&candle.high.to_string());
            concat.push(';');
            concat.push_str(&candle.low.to_string());

            result.push_str(&concat);
        }

        return result;
    }

    pub fn parse_date_time(
        candle_type: CandleType,
        partition_key: &str,
        row_key: &str,
        line: &str,
    ) -> u64 {
        match candle_type {
            CandleType::Minute => {
                let year_min = partition_key[0..4].parse::<i32>().unwrap();
                let month_min = partition_key[4..6].parse::<u32>().unwrap();
                let day_min = partition_key[6..8].parse::<u32>().unwrap();
                let hour_min = row_key[0..2].parse::<u32>().unwrap();
                let min_min = line.parse::<u32>().unwrap();
                let date_time: NaiveDateTime =
                    NaiveDate::from_ymd_opt(year_min, month_min, day_min)
                        .unwrap()
                        .and_hms_opt(hour_min, min_min, 0)
                        .unwrap();
                return date_time.timestamp() as u64;
            }
            CandleType::Hour => {
                let year_h = partition_key[0..4].parse::<i32>().unwrap();
                let month_h = partition_key[4..6].parse::<u32>().unwrap();
                let day_h = row_key[0..2].parse::<u32>().unwrap();
                let hour_h = line.parse::<u32>().unwrap();
                let date_time: NaiveDateTime = NaiveDate::from_ymd_opt(year_h, month_h, day_h)
                    .unwrap()
                    .and_hms_opt(hour_h, 0, 0)
                    .unwrap();
                return date_time.timestamp() as u64;
            }
            CandleType::Day => {
                let year_d = partition_key[0..4].parse::<i32>().unwrap();
                let month_d = row_key[4..6].parse::<u32>().unwrap();
                let day_d = line.parse::<u32>().unwrap();
                let date_time: NaiveDateTime = NaiveDate::from_ymd_opt(year_d, month_d, day_d)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                return date_time.timestamp() as u64;
            }
            CandleType::Month => {
                let year_m = partition_key[0..4].parse::<i32>().unwrap();
                let month_m = line.parse::<u32>().unwrap();
                let date_time: NaiveDateTime = NaiveDate::from_ymd_opt(year_m, month_m, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                return date_time.timestamp() as u64;
            }
        }
    }

    pub fn data_string_to_candle_grpc_model(
        src: &str,
        candle_type: CandleType,
        partition_key: &str,
        row_key: &str,
    ) -> BTreeMap<u64, CandleModel> {
        if src.len() == 0 {
            return BTreeMap::new();
        }
        let lines = src.split('|').collect::<Vec<&str>>();

        let mut result = BTreeMap::new();

        for line in lines {
            let sub_items = line.split(';').collect::<Vec<&str>>();

            let date_time = CandleModelEntity::parse_date_time(
                candle_type,
                &partition_key,
                &row_key,
                sub_items[0],
            );
            result.insert(
                date_time,
                CandleModel {
                    datetime: date_time,
                    open: sub_items[1].parse::<f64>().unwrap(),
                    close: sub_items[1].parse::<f64>().unwrap(),
                    high: sub_items[1].parse::<f64>().unwrap(),
                    low: sub_items[1].parse::<f64>().unwrap(),
                },
            );
        }

        return result;
    }
}
