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
    context.instrument_storage.restore().await;
    restore_candles(&context.clone()).await;
    //TODO: Uncomment this code
    //context.service_bus.start().await;
    // setup custom code here

    let context = application.context.clone();
    let cancellation_token = token.clone();
    let persist_candels = tokio::spawn(async move {
        let mut latest_timestamp = chrono::Utc::now().timestamp() as u64;
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

    let mut running_tasks = vec![persist_candels];

    application
        .wait_for_termination(
            sink,
            &mut running_tasks,
            Some(token.clone()),
            graceful_shutdown_func,
            1000, // how many msec wail to exeucte graceful_shutdown_func
        )
        .await;
}

async fn graceful_shutdown_func(_: Arc<AppContext>) -> bool {
    true
}
