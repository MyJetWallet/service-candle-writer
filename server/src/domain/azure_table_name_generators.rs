use crate::models::CandleType;

pub static PREFIX: &str = "CANDLE";

pub fn generate_instrument_name(instrument_id: &str) -> String {
    return format!("{PREFIX}{instrument_id}");
}

pub fn parse_short_instrument_table_name(table_name: &str) -> String {
    return if table_name.contains(PREFIX) {
        table_name.replace(PREFIX, "")
    } else {
        table_name.to_string()
    };
}

pub fn get_table_name(candle_type: CandleType, unformatted_instrument_id: &str) -> String {
    let instrument_id = unformatted_instrument_id.replace(".", "");

    if instrument_id.len() != 1 {
        return format!("{}{}", instrument_id, candle_type as i32);
    }

    let _instrument_result = generate_instrument_name(&instrument_id);

    return format!("{}{}", instrument_id, candle_type as i32);
}

pub fn parse_table_name_into_candle_and_instrument(table_name: String) -> (CandleType, String) {
    let candle_type = table_name.parse::<i32>().unwrap();
    let instrument_id = &table_name[0..table_name.len() - 1];

    let id = parse_short_instrument_table_name(instrument_id);

    match candle_type {
        0 => return (CandleType::Minute, id),
        1 => return (CandleType::Hour, id),
        2 => return (CandleType::Day, id),
        3 => return (CandleType::Month, id),
        _ => {
            tracing::error!("Invalid candle type");
            panic!("Invalid candle type")
        }
    }
}
