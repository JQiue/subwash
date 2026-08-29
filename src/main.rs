use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use once_cell::sync::Lazy;
use subwash::{get_subscription, resolve_listen_addr, resolve_refresh_interval};
use wang::{App, response::IntoResponse};

static TOKIO_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .expect("Failed to create Tokio runtime")
});

/// 成功结果缓存。
/// - 开启定时刷新时：不过期，只被成功刷新覆盖（失败保留旧值）
/// - 关闭定时刷新时：沿用 TTL，过期后由请求触发重拉
struct Cache {
  data: Option<String>,
  created_at: Option<Instant>,
  ttl: Option<Duration>,
}

impl Cache {
  fn new(ttl: Option<Duration>) -> Self {
    Self {
      ttl,
      data: None,
      created_at: None,
    }
  }

  fn set(&mut self, data: String) {
    self.data = Some(data);
    self.created_at = Some(Instant::now());
  }

  fn get(&self) -> Option<&String> {
    if self.is_expired() {
      return None;
    }
    self.data.as_ref()
  }

  fn is_expired(&self) -> bool {
    let Some(ttl) = self.ttl else {
      return false;
    };
    self
      .created_at
      .map(|time| time.elapsed() >= ttl)
      .unwrap_or(true)
  }
}

fn refresh_subscription(cache: &Arc<Mutex<Cache>>, refresh_lock: &Arc<Mutex<()>>, reason: &str) {
  let _guard = match refresh_lock.lock() {
    Ok(g) => g,
    Err(e) => {
      eprintln!("刷新锁异常: {}", e);
      return;
    }
  };

  println!("刷新订阅 [{}] ...", reason);
  match TOKIO_RT.handle().block_on(get_subscription()) {
    Ok(body) => {
      match cache.lock() {
        Ok(mut guard) => {
          guard.set(body);
          println!("刷新订阅 [{}] 成功，已写入缓存", reason);
        }
        Err(e) => eprintln!("刷新订阅 [{}] 成功但写缓存失败: {}", reason, e),
      }
    }
    Err(e) => {
      eprintln!("刷新订阅 [{}] 失败，保留旧缓存: {}", reason, e);
    }
  }
}

fn main() {
  Lazy::force(&TOKIO_RT);

  let refresh_secs = resolve_refresh_interval();
  let cache_ttl = if refresh_secs == 0 {
    // 无后台任务时，请求侧缓存 600s
    Some(Duration::from_secs(600))
  } else {
    // 有后台任务时不过期，靠定时成功结果覆盖
    None
  };

  let cache = Arc::new(Mutex::new(Cache::new(cache_ttl)));
  let refresh_lock = Arc::new(Mutex::new(()));
  let listen = resolve_listen_addr();

  if refresh_secs == 0 {
    println!("定时刷新: 关闭（config.refresh_interval = 0）");
  } else {
    println!("定时刷新: 每 {}s（启动先抓一次）", refresh_secs);
    let cache_bg = Arc::clone(&cache);
    let lock_bg = Arc::clone(&refresh_lock);
    std::thread::Builder::new()
      .name("subwash-refresh".into())
      .spawn(move || {
        loop {
          refresh_subscription(&cache_bg, &lock_bg, "timer");
          std::thread::sleep(Duration::from_secs(refresh_secs));
        }
      })
      .expect("failed to spawn refresh thread");
  }

  println!("subscribe: http://{}/subscribe", listen);

  let cache_http = Arc::clone(&cache);
  let lock_http = Arc::clone(&refresh_lock);
  App::new()
    .get("/subscribe", move |_req| {
      let cached = {
        match cache_http.lock() {
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

      // 冷启动或未开定时刷新且缓存过期：同步拉一次
      refresh_subscription(&cache_http, &lock_http, "request");

      match cache_http.lock() {
        Ok(guard) => {
          if let Some(value) = guard.get().cloned() {
            value.into_response()
          } else {
            (500u16, "failed to build subscription").into_response()
          }
        }
        Err(e) => {
          eprintln!("缓存锁异常: {}", e);
          (500u16, "failed to build subscription").into_response()
        }
      }
    })
    .listen(listen);
}
