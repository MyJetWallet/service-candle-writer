use rust_service_sdk::app::app_ctx::InitGrpc;
use rust_service_sdk::application::Application;
use service_candle_writer::app::AppContext;
use service_candle_writer::domain::{persist_candles, restore_candles};
use service_candle_writer::settings_model::SettingsModel;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let mut application = Application::<AppContext, SettingsModel>::init(AppContext::new).await;

    let clone = application.context.clone();
    let func = move |server| clone.init_grpc(server);

    let sink = application
        .start_hosting(func, "service_candle_writer".into())
        .await;

    //In case to stop application we can cancel token
    let token = Arc::new(CancellationToken::new());
    let context = application.context.clone();
    // setup custom code here

    let cancellation_token = token.clone();
    let persist_candels = tokio::spawn(async move {
        //RESTORE INSTRUMENTS
        context.instrument_storage.restore().await;

        //RESTORE CANDLES CACHE
        let mut latest_timestamp = restore_candles(&context.clone()).await;
        //START SERVICE BUS
        context.service_bus.start().await;
        loop {
            tracing::info!("persist_candels cycle started!");
            if cancellation_token.is_cancelled() {
                return Ok(());
            }
            // 1 min wait;
            tokio::time::sleep(std::time::Duration::from_millis(60_000)).await;
            let current_time = chrono::Utc::now().timestamp() as u64;

            persist_candles(&context, latest_timestamp, current_time).await;

            latest_timestamp = current_time;
            tracing::info!("persist_candels cycle ended!");
        }
    });

/*  let context = application.context.clone();
    let cancellation_token = token.clone();
    let check_size = tokio::spawn(async move {
        let mut candles_counter_min_last = 0;
        let mut candles_counter_hour_last = 0;
        let mut candles_counter_day_last = 0;
        let mut candles_counter_month_last = 0;

        loop {
            tracing::info!("check_size cycle started!");
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            let mut candles_counter_min = 0;
            let mut candles_counter_hour = 0;
            let mut candles_counter_day = 0;
            let mut candles_counter_month = 0;

            {
                let instruments = context.instrument_storage.instruments.read().await;

                tracing::info!(
                    "INSTRUMENTS COUNT: {};",
                    instruments.len()
                );
            }

            {
                let read = context.cache.ask_candles.read().await;
                read.iter().for_each(|(key, value)| {
                    candles_counter_min += value.candles_by_minute.candles.len();
                    candles_counter_hour += value.candles_by_hour.candles.len();
                    candles_counter_day += value.candles_by_day.candles.len();
                    candles_counter_month += value.candles_by_month.candles.len();
                });
            }

            {
                let read = context.cache.bid_candles.read().await;
                read.iter().for_each(|(key, value)| {
                    candles_counter_min += value.candles_by_minute.candles.len();
                    candles_counter_hour += value.candles_by_hour.candles.len();
                    candles_counter_day += value.candles_by_day.candles.len();
                    candles_counter_month += value.candles_by_month.candles.len();
                });
            }

            tracing::info!(
                "CANDLES COUNTER; MINUTE: {}; HOUR: {}; DAY: {}; MONTH: {};",
                candles_counter_min,
                candles_counter_hour,
                candles_counter_day,
                candles_counter_month
            );

            tracing::info!(
                "CANDLES GROWTH; MINUTE: {}; HOUR: {}; DAY: {}; MONTH: {};",
                candles_counter_min - candles_counter_min_last,
                candles_counter_hour - candles_counter_hour_last,
                candles_counter_day - candles_counter_day_last,
                candles_counter_month - candles_counter_month_last
            );

            candles_counter_min_last = candles_counter_min;
            candles_counter_hour_last = candles_counter_hour;
            candles_counter_day_last = candles_counter_day;
            candles_counter_month_last = candles_counter_month;
            // 1 min wait;
            tracing::info!("check_size cycle ended!");
            tokio::time::sleep(std::time::Duration::from_millis(60_000)).await;
        }
    }); */

    let mut running_tasks = vec![persist_candels, /* check_size */];

    application
        .wait_for_termination(
            sink,
            &mut running_tasks,
            Some(token.clone()),
            graceful_shutdown_func,
            1500, // how many msec wail to exeucte graceful_shutdown_func
        )
        .await;
}

async fn graceful_shutdown_func(_: Arc<AppContext>) -> bool {
    true
}
