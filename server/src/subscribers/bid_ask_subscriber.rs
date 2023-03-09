use std::sync::Arc;

use my_service_bus_abstractions::subscriber::{
    MessagesReader, MySbSubscriberHandleError, SubscriberCallback,
};
use my_service_bus_tcp_client::MyServiceBusClient;
use service_candle_writer_generated_proto::{BidAsk, CandleMessage, CandleGroup, CandleItem};

use crate::{
    caches::CandlesInstrumentsCache,
    models::{CandlesBidAsk}, domain::InstrumentStorage,
};
pub struct BidAskSubscriber {
    pub cache: Arc<CandlesInstrumentsCache>,
    pub service_bus: Arc<MyServiceBusClient>,
    pub instrument_storage: Arc<InstrumentStorage>,
}

impl BidAskSubscriber {
    pub fn new(
        cache: Arc<CandlesInstrumentsCache>,
        service_bus: Arc<MyServiceBusClient>,
        instrument_storage: Arc<InstrumentStorage>,
    ) -> Self {
        Self {
            cache,
            service_bus,
            instrument_storage,
        }
    }
}

#[async_trait::async_trait]
impl SubscriberCallback<BidAsk> for BidAskSubscriber {
    async fn handle_messages(
        &self,
        messages_reader: &mut MessagesReader<BidAsk>,
    ) -> Result<(), MySbSubscriberHandleError> {

        while let Some(message) = messages_reader.get_next_message() {
            let message: CandlesBidAsk = message.take_message().into();
            let instrument = message.instrument.clone();
            tracing::info!("Handled bid ask: {:?}", message);
            
            if !self.instrument_storage.contains(&instrument).await {
                self.instrument_storage.add(instrument.clone()).await;
            }
            
            let (bid, ask) = self.cache
            .update_once(message).await;

            let publisher = self.service_bus.get_publisher::<CandleMessage>(true).await;
            
            let to_transfer = CandleMessage {
                instrument: instrument.clone(),
                unix_time_sec: bid.0.1.datetime,
                ask: Some(CandleGroup {
                    minute: Some (CandleItem {
                        open: ask.0.1.open,
                        close: ask.0.1.close,
                        high: ask.0.1.high,
                        low: ask.0.1.low,
                    }),
                    hour: Some (CandleItem {
                        open: ask.1.1.open,
                        close: ask.1.1.close,
                        high: ask.1.1.high,
                        low: ask.1.1.low,
                    }),
                    day: Some (CandleItem {
                        open: ask.2.1.open,
                        close: ask.2.1.close,
                        high: ask.2.1.high,
                        low: ask.2.1.low,
                    }),
                    month: Some (CandleItem {
                        open: ask.3.1.open,
                        close: ask.3.1.close,
                        high: ask.3.1.high,
                        low: ask.3.1.low,
                    }),
                }),
                bid: Some(CandleGroup {
                    minute: Some (CandleItem {
                        open: bid.0.1.open,
                        close: bid.0.1.close,
                        high: bid.0.1.high,
                        low: bid.0.1.low,
                    }),
                    hour: Some (CandleItem {
                        open: bid.1.1.open,
                        close: bid.1.1.close,
                        high: bid.1.1.high,
                        low: bid.1.1.low,
                    }),
                    day: Some (CandleItem {
                        open: bid.2.1.open,
                        close: bid.2.1.close,
                        high: bid.2.1.high,
                        low: bid.2.1.low,
                    }),
                    month: Some (CandleItem {
                        open: bid.3.1.open,
                        close: bid.3.1.close,
                        high: bid.3.1.high,
                        low: bid.3.1.low,
                    }),
                }),
            };

            publisher.publish(&to_transfer).await.unwrap();
        }

        Ok(())
    }
}
