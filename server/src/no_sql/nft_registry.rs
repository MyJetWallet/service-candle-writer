use serde::{Serialize, Deserialize};

/// Service.Nft.Manager.Domain.Models.NftRecordRegistryNoSqlEntity

#[derive(Serialize, Deserialize, Debug)]
pub struct NftRecordRegistryNoSqlEntity {
    #[serde(rename = "PartitionKey")]
    pub partition_key: String,
    #[serde(rename = "RowKey")]
    pub row_key: String,
    #[serde(rename = "TimeStamp")]
    pub time_stamp: String,
    #[serde(rename = "NftRecord")]
    pub nft_record: NftRecord,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct NftRecord {
    #[serde(rename = "ContractAddress")]
    pub contract_address: String
}

impl my_no_sql_server_abstractions::MyNoSqlEntity for NftRecordRegistryNoSqlEntity {
    fn get_partition_key(&self) -> &str {
        &self.partition_key[..]
    }
    fn get_row_key(&self) -> &str {
        &self.row_key[..]
    }
    fn get_time_stamp(&self) -> i64 {
        0
    }

    const TABLE_NAME: &'static str = "myjetwallet-nft-manager-registry";
}
