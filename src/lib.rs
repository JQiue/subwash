use std::{
  collections::{BTreeMap, HashMap},
  fs,
  net::SocketAddr,
  time::Duration,
};

use reqwest::header::USER_AGENT;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use surge_ping::{Client, PingIdentifier, PingSequence};
use tokio::{
  net::{TcpStream, UdpSocket},
  time::{Instant, timeout},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum SubscriptionTier {
  Premium,
  #[default]
  Standard,
  Backup,
}

#[derive(Debug, Deserialize)]
struct Config {
  speed_test: u64,
  exclude_keyword: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
  config: Config,
  subscriptions: Vec<Subscription>,
}

#[derive(Debug, Deserialize, Clone)]
struct Subscription {
  name: String,
  url: String,
  tier: SubscriptionTier,
  max_nodes: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ProxyNode {
  pub name: String,
  pub r#type: String,
  pub server: String,
  pub port: u16,

  #[serde(skip)]
  source_tier: SubscriptionTier,
  #[serde(skip)]
  group_name: String,
  #[serde(skip)]
  latency: Option<u64>,

  #[serde(flatten)]
  pub other_fields: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MySubscritption {
  proxies: Vec<ProxyNode>,
  #[serde(rename = "proxy-groups")]
  proxy_groups: Vec<ProxyGroup>,
  #[serde(flatten)]
  other_fields: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProxyGroup {
  name: String,
  r#type: String,
  proxies: Vec<String>,
  #[serde(flatten)]
  other_fields: BTreeMap<String, Value>,
}

pub async fn get_subscription() -> Result<String, Box<dyn std::error::Error>> {
  let config_content = match std::fs::read_to_string("config.yaml") {
    Ok(content) => content,
    Err(e) => {
      eprintln!("读取 config.yaml 失败，请确保该文件存在: {}", e);
      return Err(Box::new(e));
    }
  };

  let config: AppConfig = match serde_yaml::from_str(&config_content) {
    Ok(cfg) => cfg,
    Err(e) => {
      eprintln!("解析 config.yaml 失败，格式有误: {}", e);
      return Err(Box::new(e));
    }
  };

  let subscriptions = config.subscriptions.clone();

  let mut all_nodes = Vec::new();

  for sub in &subscriptions {
    let nodes = fetch_and_decode_sub(sub).await;
    all_nodes.extend(nodes);
  }

  if let Some(ref exclude_keyword) = config.config.exclude_keyword
    && !exclude_keyword.is_empty()
  {
    all_nodes.retain(|node| {
      let should_discard = exclude_keyword
        .iter()
        .any(|keyword| node.name.contains(keyword));
      !should_discard
    });
  }

  test_speed(&mut all_nodes, config.config.speed_test).await;

  let alive_nodes: Vec<ProxyNode> = all_nodes
    .into_iter()
    .filter(|node| node.latency.is_some())
    .collect();

  let mut grouped_nodes: HashMap<String, Vec<ProxyNode>> = HashMap::new();
  for node in alive_nodes {
    grouped_nodes
      .entry(node.group_name.clone())
      .or_default()
      .push(node);
  }

  let mut final_nodes = Vec::new();

  for sub in &config.subscriptions {
    if let Some(mut nodes) = grouped_nodes.remove(&sub.name) {
      // 按延迟 (latency) 从小到大排序 (升序)
      nodes.sort_by_key(|node| node.latency.unwrap_or(u64::MAX));

      if let Some(limit) = sub.max_nodes
        && nodes.len() > limit
      {
        println!(
          "机场 [{}] 存活节点数 {}，触发限额，仅保留延迟最低的前 {} 个",
          sub.name,
          nodes.len(),
          limit
        );
        nodes.truncate(limit); // 截取前 limit 个节点
      }

      for node in &nodes {
        println!(
          "   -> [保留] 机场: {} | 节点: {:<15} | 延迟: {}ms",
          sub.name,
          node.name,
          node.latency.unwrap()
        );
      }

      final_nodes.extend(nodes);
    }
  }

  all_nodes = final_nodes;

  let mut select_group_proxies = vec!["DIRECT".to_string(), "自动选择".to_string()];

  for sub in &subscriptions {
    let has_alive_nodes = all_nodes.iter().any(|node| node.group_name == sub.name);
    if has_alive_nodes {
      select_group_proxies.push(sub.name.clone());
    }
  }

  let mut auto_select_group_proxies = vec![];

  subscriptions.iter().for_each(|sub| {
    if sub.tier == SubscriptionTier::Standard {
      auto_select_group_proxies.push(sub.name.clone());
    }
  });

  let mut proxy_groups = vec![
    ProxyGroup {
      name: "节点选择".to_string(),
      r#type: "select".to_string(),
      proxies: select_group_proxies,
      other_fields: BTreeMap::new(),
    },
    ProxyGroup {
      name: "自动选择".to_string(),
      r#type: "url-test".to_string(),
      proxies: auto_select_group_proxies,
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(600).unwrap());
        map.insert("tolerance".to_string(), serde_yaml::to_value(200).unwrap());
        map
      },
    },
    ProxyGroup {
      name: "故障转移".to_string(),
      r#type: "fallback".to_string(),
      proxies: subscriptions
        .iter()
        .filter_map(|sub| {
          if sub.tier == SubscriptionTier::Premium {
            Some(sub.name.clone())
          } else {
            None
          }
        })
        .collect(),
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(700).unwrap());
        map.insert("lazy".to_string(), serde_yaml::to_value(true).unwrap());
        map
      },
    },
    ProxyGroup {
      name: "Gemini".to_string(),
      r#type: "select".to_string(),
      proxies: subscriptions
        .iter()
        .filter_map(|sub| {
          if sub.tier == SubscriptionTier::Premium {
            Some(sub.name.clone())
          } else {
            None
          }
        })
        .collect(),
      other_fields: BTreeMap::new(),
    },
    ProxyGroup {
      name: "Steam".to_string(),
      r#type: "select".to_string(),
      proxies: vec!["DIRECT".to_string()],
      other_fields: BTreeMap::new(),
    },
  ];

  subscriptions.iter().for_each(|sub| {
    proxy_groups.push(ProxyGroup {
      name: sub.name.clone(),
      r#type: "url-test".to_string(),
      proxies: all_nodes
        .iter()
        .filter(|node| node.group_name == sub.name)
        .map(|node| node.name.clone())
        .collect(),
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(600).unwrap());
        map.insert("tolerance".to_string(), serde_yaml::to_value(200).unwrap());
        map
      },
    });
  });

  let text = fs::read_to_string("./template.yaml").unwrap();
  let mut my_subscritption = serde_yaml::from_str::<MySubscritption>(&text).unwrap();
  my_subscritption.proxies = all_nodes;
  my_subscritption.proxy_groups = proxy_groups;
  let text = serde_yaml::to_string(&my_subscritption).unwrap();
  Ok(text)
}

#[tokio::main]
async fn main() {
  let config_content = match std::fs::read_to_string("config.yaml") {
    Ok(content) => content,
    Err(e) => {
      eprintln!("读取 config.yaml 失败，请确保该文件存在: {}", e);
      return;
    }
  };

  let config: AppConfig = match serde_yaml::from_str(&config_content) {
    Ok(cfg) => cfg,
    Err(e) => {
      eprintln!("解析 config.yaml 失败，格式有误: {}", e);
      return;
    }
  };

  let subscriptions = config.subscriptions.clone();

  let mut all_nodes = Vec::new();

  for sub in &subscriptions {
    let nodes = fetch_and_decode_sub(sub).await;
    all_nodes.extend(nodes);
  }

  if let Some(ref exclude_keyword) = config.config.exclude_keyword
    && !exclude_keyword.is_empty()
  {
    all_nodes.retain(|node| {
      let should_discard = exclude_keyword
        .iter()
        .any(|keyword| node.name.contains(keyword));
      !should_discard
    });
  }

  test_speed(&mut all_nodes, config.config.speed_test).await;

  let alive_nodes: Vec<ProxyNode> = all_nodes
    .into_iter()
    .filter(|node| node.latency.is_some())
    .collect();

  let mut grouped_nodes: HashMap<String, Vec<ProxyNode>> = HashMap::new();
  for node in alive_nodes {
    grouped_nodes
      .entry(node.group_name.clone())
      .or_default()
      .push(node);
  }

  let mut final_nodes = Vec::new();

  for sub in &config.subscriptions {
    if let Some(mut nodes) = grouped_nodes.remove(&sub.name) {
      // 按延迟 (latency) 从小到大排序 (升序)
      nodes.sort_by_key(|node| node.latency.unwrap_or(u64::MAX));

      if let Some(limit) = sub.max_nodes
        && nodes.len() > limit
      {
        println!(
          "机场 [{}] 存活节点数 {}，触发限额，仅保留延迟最低的前 {} 个",
          sub.name,
          nodes.len(),
          limit
        );
        nodes.truncate(limit); // 截取前 limit 个节点
      }

      for node in &nodes {
        println!(
          "   -> [保留] 机场: {} | 节点: {:<15} | 延迟: {}ms",
          sub.name,
          node.name,
          node.latency.unwrap()
        );
      }

      final_nodes.extend(nodes);
    }
  }

  all_nodes = final_nodes;

  let mut select_group_proxies = vec!["DIRECT".to_string(), "自动选择".to_string()];

  for sub in &subscriptions {
    let has_alive_nodes = all_nodes.iter().any(|node| node.group_name == sub.name);
    if has_alive_nodes {
      select_group_proxies.push(sub.name.clone());
    }
  }

  let mut auto_select_group_proxies = vec![];

  subscriptions.iter().for_each(|sub| {
    if sub.tier == SubscriptionTier::Standard {
      auto_select_group_proxies.push(sub.name.clone());
    }
  });

  let mut proxy_groups = vec![
    ProxyGroup {
      name: "节点选择".to_string(),
      r#type: "select".to_string(),
      proxies: select_group_proxies,
      other_fields: BTreeMap::new(),
    },
    ProxyGroup {
      name: "自动选择".to_string(),
      r#type: "url-test".to_string(),
      proxies: auto_select_group_proxies,
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(600).unwrap());
        map.insert("tolerance".to_string(), serde_yaml::to_value(200).unwrap());
        map
      },
    },
    ProxyGroup {
      name: "故障转移".to_string(),
      r#type: "fallback".to_string(),
      proxies: subscriptions
        .iter()
        .filter_map(|sub| {
          if sub.tier == SubscriptionTier::Premium {
            Some(sub.name.clone())
          } else {
            None
          }
        })
        .collect(),
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(700).unwrap());
        map.insert("lazy".to_string(), serde_yaml::to_value(true).unwrap());
        map
      },
    },
    ProxyGroup {
      name: "Gemini".to_string(),
      r#type: "select".to_string(),
      proxies: subscriptions
        .iter()
        .filter_map(|sub| {
          if sub.tier == SubscriptionTier::Premium {
            Some(sub.name.clone())
          } else {
            None
          }
        })
        .collect(),
      other_fields: BTreeMap::new(),
    },
    ProxyGroup {
      name: "Steam".to_string(),
      r#type: "select".to_string(),
      proxies: vec!["DIRECT".to_string()],
      other_fields: BTreeMap::new(),
    },
  ];

  subscriptions.iter().for_each(|sub| {
    proxy_groups.push(ProxyGroup {
      name: sub.name.clone(),
      r#type: "url-test".to_string(),
      proxies: all_nodes
        .iter()
        .filter(|node| node.group_name == sub.name)
        .map(|node| node.name.clone())
        .collect(),
      other_fields: {
        let mut map = BTreeMap::new();
        map.insert(
          "url".to_string(),
          serde_yaml::to_value("http://www.YouTube.com").unwrap(),
        );
        map.insert("interval".to_string(), serde_yaml::to_value(600).unwrap());
        map.insert("tolerance".to_string(), serde_yaml::to_value(200).unwrap());
        map
      },
    });
  });

  let text = fs::read_to_string("./template.yaml").unwrap();
  let mut my_subscritption = serde_yaml::from_str::<MySubscritption>(&text).unwrap();
  my_subscritption.proxies = all_nodes;
  my_subscritption.proxy_groups = proxy_groups;
  let text = serde_yaml::to_string(&my_subscritption).unwrap();
  fs::write("./clash_config.yaml", text).unwrap();
}

async fn fetch_and_decode_sub(sub: &Subscription) -> Vec<ProxyNode> {
  let client = reqwest::Client::new();
  let resp = client
    .get(sub.url.clone())
    .header(USER_AGENT, "clash-verge/v2.5.1")
    .send()
    .await;

  let mut result_nodes = Vec::new();

  if let Ok(resp) = resp {
    let text = resp.text().await.unwrap();
    let proxy_nodes = extract_proxies_from_yaml(&text);

    for mut node in proxy_nodes {
      node.source_tier = sub.tier.clone();
      node.group_name = sub.name.clone();
      result_nodes.push(node);
    }
  }

  result_nodes
}

async fn test_speed(proxy_nodes: &mut Vec<ProxyNode>, latency: u64) {
  let mut tasks = vec![];
  for node in proxy_nodes.drain(..) {
    let handle = tokio::spawn(async move {
      ping_icmp(&node.server, latency)
        .await
        .map(|rtt| (node, rtt))
    });
    tasks.push(handle);
  }

  let mut active_results = vec![];

  for task in tasks {
    if let Ok(Some((mut node, rtt))) = task.await {
      // 打印一下测速成功的节点，方便调试
      println!("节点: {:<20} | 延迟: {}ms", node.name, rtt);
      node.latency = Some(rtt);
      active_results.push((node, rtt));
    }
  }

  for (node, _) in active_results {
    proxy_nodes.push(node);
  }

  println!(
    "\n测速完成！可用节点剩余: {}/{}",
    proxy_nodes.len(),
    proxy_nodes.capacity()
  );
}

async fn ping_icmp(host: &str, timeout_limit: u64) -> Option<u64> {
  let addrs = tokio::net::lookup_host(format!("{}:80", host)).await.ok()?;
  let ip = addrs.into_iter().next()?.ip();

  let client = Client::new(&surge_ping::Config::default()).ok()?;
  let mut pinger = client.pinger(ip, PingIdentifier(111)).await;

  let payload = [0u8; 16]; // 16字节的轻量填充数据
  match tokio::time::timeout(
    Duration::from_millis(timeout_limit),
    pinger.ping(PingSequence(1), &payload),
  )
  .await
  {
    Ok(Ok((_, rtt))) => Some(rtt.as_millis() as u64),
    _ => None,
  }
}

pub async fn ping_protocol(
  node_type: &str,
  server: &str,
  port: u16,
  timeout_limit_ms: u64,
) -> Option<u64> {
  let addr_str = format!("{}:{}", server, port);
  let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&addr_str).await.ok()?.collect();
  let addr = *addrs.first()?;

  let start = Instant::now();
  let duration_limit = Duration::from_millis(timeout_limit_ms);

  let node_type_lower = node_type.to_lowercase();

  if node_type_lower == "hysteria2" || node_type_lower == "tuic" {
    match timeout(duration_limit, async {
      let socket = UdpSocket::bind("0.0.0.0:0").await?;
      socket.connect(addr).await?;

      let dummy_payload = [0u8; 12];
      socket.send(&dummy_payload).await?;

      let mut buf = [0u8; 1];

      match socket.recv(&mut buf).await {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionRefused => Err(
          std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "port closed"),
        ),
        _ => Ok(()),
      }
    })
    .await
    {
      Ok(Ok(_)) => Some(start.elapsed().as_millis() as u64),
      _ => None,
    }
  } else {
    // ==================== 🔴 TCP 协议探测 (Vmess, SS, Trojan) ====================
    match timeout(duration_limit, TcpStream::connect(addr)).await {
      Ok(Ok(_stream)) => Some(start.elapsed().as_millis() as u64),
      _ => None,
    }
  }
}

#[derive(Deserialize, Debug)]
struct ClashConfig {
  proxies: Vec<ProxyNode>,
}

fn extract_proxies_from_yaml(text: &str) -> Vec<ProxyNode> {
  let clash_config = serde_yaml::from_str::<ClashConfig>(text).unwrap();
  clash_config.proxies
}
