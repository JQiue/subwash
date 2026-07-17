use once_cell::sync::Lazy;
use subwash::get_subscription;
use wang::{App, response::IntoResponse};

static TOKIO_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

fn main() {
    Lazy::force(&TOKIO_RT);
    App::new()
        .get("/subscribe", |_req| {
            let handle = TOKIO_RT.handle();
            let clash_config_string = handle.block_on(async { get_subscription().await }).unwrap();
            clash_config_string.into_response()
        })
        .listen("127.0.0.1:5000");
}
