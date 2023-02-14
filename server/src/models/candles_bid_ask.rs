use service_candle_writer_generated_proto::BidAsk;

#[derive(Debug, Clone)]
pub struct CandlesBidAsk{
    pub date: u64,
    pub instrument: String,
    pub bid: f64,
    pub ask: f64,
}

impl From<BidAsk> for CandlesBidAsk {
    fn from(bid_ask: BidAsk) -> Self {
        CandlesBidAsk {
            date: bid_ask.unix_time_sec,
            instrument: bid_ask.id,
            bid: bid_ask.bid,
            ask: bid_ask.ask,
        }
    }
}