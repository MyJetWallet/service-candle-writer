use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SettingsModel {
    #[serde(rename = "CandleWriterRust")]
    pub inner: SettingsModelInner,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SettingsModelInner {
    #[serde(rename = "LogStashUrl")]
    pub log_stash_url: String,

    #[serde(rename = "MyNoSqlWriterUrl")]
    pub my_no_sql_writer_url: String,
    
    #[serde(rename = "MyNoSqlReaderHostPort")]
    pub my_no_sql_reader_host_port: String,

    #[serde(rename = "SpotServiceBusHostPort")]
    pub spot_service_bus_hos_port: String,

    #[serde(rename = "MinuteLimit")]
    pub minute_limit: usize,

    #[serde(rename = "HourLimit")]
    pub hour_limit: usize,

    #[serde(rename = "AzureStorageAccountAsk")]
    pub azure_storage_account_ask: String,

    #[serde(rename = "AzureStorageAccessKeyAsk")]
    pub azure_storage_access_key_ask: String,

    #[serde(rename = "AzureStorageAccountBid")]
    pub azure_storage_account_bid: String,

    #[serde(rename = "AzureStorageAccessKeyBid")]
    pub azure_storage_access_key_bid: String,
}

impl rust_service_sdk::app::app_ctx::GetLogStashUrl for SettingsModel {
    fn get_logstash_url(&self) -> String {
        self.inner.log_stash_url.clone()
        //USE ONLY CONSOLE SINK: "".to_string()
    }
}
