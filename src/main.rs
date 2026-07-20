use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use subwash::get_subscription;
use wang::{App, response::IntoResponse};

static TOKIO_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .expect("Failed to create Tokio runtime")
});

struct Cache<T> {
  data: Option<T>,
  created_at: Option<Instant>,
  ttl: Duration,
}

impl<T> Cache<T> {
  fn new(ttl: Duration) -> Self {
    Self {
      ttl,
      data: None,
      created_at: None,
    }
  }

  fn set(&mut self, data: T) {
    self.data = Some(data);
    self.created_at = Some(Instant::now());
  }

  fn get(&self) -> Option<&T> {
    if self.is_expired() {
      return None;
    }

    self.data.as_ref()
  }

  // 判断是否过期
  fn is_expired(&self) -> bool {
    self
      .created_at
      .map(|time| time.elapsed() >= self.ttl)
      .unwrap_or(true)
  }

  fn _clear(&mut self) {
    self.data = None;
    self.created_at = None;
  }
}

fn main() {
  Lazy::force(&TOKIO_RT);
  let cache = Arc::new(Mutex::new(Cache::<String>::new(Duration::from_secs(600))));
  App::new()
    .get("/subscribe", move |_req| {
      let cached = {
        let cache = cache.lock().unwrap();
        cache.get().cloned()
      };

      if let Some(value) = cached {
        return value.into_response();
      }

      let handle = TOKIO_RT.handle();
      let clash_config_string = handle.block_on(async { get_subscription().await }).unwrap();
      {
        let mut cache = cache.lock().unwrap();
        cache.set(clash_config_string.clone());
      }
      clash_config_string.into_response()
    })
    .listen("127.0.0.1:5000");
}
