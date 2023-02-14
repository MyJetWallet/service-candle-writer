use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockReaderNoSql {
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "RowKey")]
    pub row_key: String,
    #[serde(rename = "TimeStamp")]
    pub time_stamp: String,
    #[serde(rename = "filter")]
    pub filter: String,
}

impl my_no_sql_server_abstractions::MyNoSqlEntity for BlockReaderNoSql {
    fn get_partition_key(&self) -> &str {
        &self.partition_key[..]
    }
    fn get_row_key(&self) -> &str {
        &self.row_key[..]
    }
    fn get_time_stamp(&self) -> i64 {
        0
    }

    const TABLE_NAME: &'static str = "service-candle-writer-block-reader";
}
