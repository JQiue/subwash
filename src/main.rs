use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use subwash::{get_subscription, resolve_listen_addr};
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

  fn is_expired(&self) -> bool {
    self
      .created_at
      .map(|time| time.elapsed() >= self.ttl)
      .unwrap_or(true)
  }
}

fn main() {
  Lazy::force(&TOKIO_RT);
  let cache = Arc::new(Mutex::new(Cache::<String>::new(Duration::from_secs(600))));
  let listen = resolve_listen_addr();
  println!("subscribe: http://{}/subscribe", listen);

  App::new()
    .get("/subscribe", move |_req| {
      let cached = {
        match cache.lock() {
          Ok(guard) => guard.get().cloned(),
          Err(e) => {
            eprintln!("缓存锁异常: {}", e);
            None
          }
        }
      };

      if let Some(value) = cached {
        return value.into_response();
      }

      let handle = TOKIO_RT.handle();
      match handle.block_on(get_subscription()) {
        Ok(clash_config_string) => {
          if let Ok(mut guard) = cache.lock() {
            guard.set(clash_config_string.clone());
          }
          clash_config_string.into_response()
        }
        Err(e) => {
          eprintln!("生成订阅失败: {}", e);
          // 失败不写缓存，避免 600s 内一直返回坏结果
          (500u16, "failed to build subscription").into_response()
        }
      }
    })
    .listen(listen);
}
