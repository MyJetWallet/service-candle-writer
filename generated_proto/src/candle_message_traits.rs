use my_service_bus_abstractions::publisher::MySbMessageSerializer;
use my_service_bus_abstractions::{subscriber::MySbMessageDeserializer, GetMySbModelTopicId};

use crate::CandleMessage;


impl MySbMessageDeserializer for CandleMessage {
    type Item = CandleMessage;

    fn deserialize(
        src: &[u8],
        _headers: &Option<std::collections::HashMap<String, String>>,
    ) -> Result<Self::Item, my_service_bus_abstractions::SubscriberError> {
        //implement

        let transfer_event_message = prost::Message::decode(&src[1..]);
        let transfer_event: CandleMessage;

        match transfer_event_message {
            Ok(x) => transfer_event = x,
            Err(err) => {
                tracing::error!("Can't deserialize CandleMessage: {:?}", err);
                return Err(
                    my_service_bus_abstractions::SubscriberError::CanNotDeserializeMessage(
                        err.to_string(),
                    ),
                );
            }
        }

        Ok(transfer_event)
    }
}

impl MySbMessageSerializer for CandleMessage {
    fn serialize(
        &self,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> Result<(Vec<u8>, Option<std::collections::HashMap<String, String>>), String> {
        let mut buf = vec![0];
        let encode_res = prost::Message::encode(self, &mut buf);

        match encode_res {
            Ok(_) => Ok((buf, headers)),
            Err(err) => Err(err.to_string()),
        }
    }
}

pub static CONFIRMED_TOPIC: &str = "service-candle-writer-candles";

impl GetMySbModelTopicId for CandleMessage {
    fn get_topic_id() -> &'static str {
        CONFIRMED_TOPIC
    }
}
