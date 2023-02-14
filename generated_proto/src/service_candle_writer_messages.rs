#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BidAsk {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// string date_time = 2;
    #[prost(double, tag = "3")]
    pub bid: f64,
    #[prost(double, tag = "4")]
    pub ask: f64,
    #[prost(uint64, tag = "5")]
    pub unix_time_sec: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CandleMessage {
    #[prost(string, tag = "1")]
    pub instrument: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub unix_time_sec: u64,
    #[prost(message, optional, tag = "3")]
    pub bid: ::core::option::Option<CandleGroup>,
    #[prost(message, optional, tag = "4")]
    pub ask: ::core::option::Option<CandleGroup>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CandleGroup {
    #[prost(message, optional, tag = "1")]
    pub minute: ::core::option::Option<CandleItem>,
    #[prost(message, optional, tag = "2")]
    pub hour: ::core::option::Option<CandleItem>,
    #[prost(message, optional, tag = "3")]
    pub day: ::core::option::Option<CandleItem>,
    #[prost(message, optional, tag = "4")]
    pub month: ::core::option::Option<CandleItem>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CandleItem {
    #[prost(double, tag = "4")]
    pub open: f64,
    #[prost(double, tag = "6")]
    pub high: f64,
    #[prost(double, tag = "8")]
    pub low: f64,
    #[prost(double, tag = "10")]
    pub close: f64,
}
