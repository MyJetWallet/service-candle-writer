mod database;
mod request_counter;
mod instrument_storage;
mod azure_table_name_generators;

pub use database::Database;
pub use request_counter::DatabaseImpl;
pub use request_counter::RequestCounter;
pub use instrument_storage::InstrumentStorage;

pub use database::persist_candles;
pub use database::restore_candles;
pub use database::CandlesPersistentAzureStorage;

pub use azure_table_name_generators::*;