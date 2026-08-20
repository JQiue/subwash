use std::{
  collections::{BTreeMap, HashMap, VecDeque},
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

/// 节点截取策略。订阅级可覆盖全局 `config.select_mode`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum SelectMode {
  /// 按节点名识别地区分桶，先保各地区名额，再填满 `max_nodes`（默认）。
  #[default]
  RegionDiverse,
  /// 全局按延迟从低到高截取（旧行为）。
  LatencyTop,
}

#[derive(Debug, Deserialize)]
struct Config {
  speed_test: u64,
  exclude_keyword: Option<Vec<String>>,
  /// 默认截取策略；可被订阅项覆盖。
  select_mode: Option<SelectMode>,
  /// `region_diverse`：每个地区保底保留数，默认 1。
  min_per_region: Option<usize>,
  /// `region_diverse`：每个地区上限；缺省不限制（仍受 `max_nodes` 约束）。
  max_per_region: Option<usize>,
  /// HTTP 监听地址，默认 `127.0.0.1:5000`；可被环境变量 `SUBWASH_LISTEN` 覆盖。
  listen: Option<String>,
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
  select_mode: Option<SelectMode>,
  min_per_region: Option<usize>,
  max_per_region: Option<usize>,
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

/// 订阅响应头 `subscription-userinfo` 解析结果（字节 / Unix 秒）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SubscriptionTraffic {
  upload: Option<u64>,
  download: Option<u64>,
  total: Option<u64>,
  expire: Option<i64>,
}

struct FetchResult {
  name: String,
  nodes: Vec<ProxyNode>,
  traffic: Option<SubscriptionTraffic>,
}

/// 解析监听地址：`SUBWASH_LISTEN` > `config.yaml` 的 `config.listen` > 默认。
pub fn resolve_listen_addr() -> String {
  if let Ok(addr) = std::env::var("SUBWASH_LISTEN") {
    let addr = addr.trim();
    if !addr.is_empty() {
      return addr.to_string();
    }
  }

  if let Ok(content) = std::fs::read_to_string("config.yaml")
    && let Ok(cfg) = serde_yaml::from_str::<AppConfig>(&content)
    && let Some(addr) = cfg.config.listen
  {
    let addr = addr.trim();
    if !addr.is_empty() {
      return addr.to_string();
    }
  }

  "127.0.0.1:5000".to_string()
}

pub async fn get_subscription() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
  let config_content = std::fs::read_to_string("config.yaml").map_err(|e| {
    eprintln!("读取 config.yaml 失败，请确保该文件存在: {}", e);
    e
  })?;

  let config: AppConfig = serde_yaml::from_str(&config_content).map_err(|e| {
    eprintln!("解析 config.yaml 失败，格式有误: {}", e);
    e
  })?;

  let subscriptions = config.subscriptions.clone();

  let mut fetch_handles = Vec::with_capacity(subscriptions.len());
  for sub in &subscriptions {
    let sub = sub.clone();
    fetch_handles.push(tokio::spawn(
      async move { fetch_and_decode_sub(&sub).await },
    ));
  }

  let mut all_nodes = Vec::new();
  // 供「订阅信息」展示组；按订阅名索引，组装时再按 config 顺序输出
  let mut traffic_by_name: HashMap<String, SubscriptionTraffic> = HashMap::new();
  for handle in fetch_handles {
    match handle.await {
      Ok(FetchResult {
        name,
        nodes,
        traffic,
      }) => {
        if let Some(t) = traffic {
          traffic_by_name.insert(name, t);
        }
        all_nodes.extend(nodes);
      }
      Err(e) => eprintln!("拉取订阅任务异常: {}", e),
    }
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
  let default_mode = config.config.select_mode.unwrap_or_default();
  let default_min_per_region = config.config.min_per_region.unwrap_or(1);
  let default_max_per_region = config.config.max_per_region;

  for sub in &config.subscriptions {
    if let Some(nodes) = grouped_nodes.remove(&sub.name) {
      let alive_count = nodes.len();
      let mode = sub.select_mode.unwrap_or(default_mode);
      let min_per_region = sub.min_per_region.unwrap_or(default_min_per_region);
      let max_per_region = sub.max_per_region.or(default_max_per_region);
      let nodes =
        select_subscription_nodes(nodes, sub.max_nodes, mode, min_per_region, max_per_region);

      if let Some(limit) = sub.max_nodes
        && alive_count > nodes.len()
      {
        println!(
          "机场 [{}] 存活节点数 {}，策略 {:?}，限额 {}，保留 {} 个",
          sub.name,
          alive_count,
          mode,
          limit,
          nodes.len()
        );
      }

      final_nodes.extend(nodes);
    }
  }

  // 多机场合并前加前缀，避免 Clash proxy 同名覆盖
  uniquify_proxy_names(&mut final_nodes);

  for node in &final_nodes {
    println!(
      "   -> [保留] 机场: {} | 地区: {:<6} | 节点: {:<24} | 延迟: {}ms",
      node.group_name,
      detect_region(&node.name),
      node.name,
      node.latency.unwrap_or(u64::MAX)
    );
  }

  let all_nodes = final_nodes;

  // 仅保留「至少有一个存活节点」的机场，避免 url-test/fallback 的 proxies 为空导致 Clash 校验失败
  let alive_subs: Vec<&Subscription> = subscriptions
    .iter()
    .filter(|sub| all_nodes.iter().any(|node| node.group_name == sub.name))
    .collect();

  if alive_subs.len() < subscriptions.len() {
    for sub in &subscriptions {
      if !alive_subs.iter().any(|s| s.name == sub.name) {
        eprintln!(
          "机场 [{}] 无可用节点，跳过对应策略组（拉取失败 / 全被过滤 / 测速全灭）",
          sub.name
        );
      }
    }
  }

  if alive_subs.is_empty() {
    return Err("所有订阅均无可用节点，拒绝生成空配置".into());
  }

  let auto_select_group_proxies: Vec<String> = alive_subs
    .iter()
    .filter(|sub| sub.tier == SubscriptionTier::Standard)
    .map(|sub| sub.name.clone())
    .collect();

  let premium_group_proxies: Vec<String> = alive_subs
    .iter()
    .filter(|sub| sub.tier == SubscriptionTier::Premium)
    .map(|sub| sub.name.clone())
    .collect();

  // 故障转移：跨机场兜底。顺序：自动选择(聚合 Standard) → 各 Premium → 其余未覆盖机场
  let mut fallback_group_proxies: Vec<String> = Vec::new();
  if !auto_select_group_proxies.is_empty() {
    fallback_group_proxies.push("自动选择".to_string());
  }
  for name in &premium_group_proxies {
    fallback_group_proxies.push(name.clone());
  }
  for sub in &alive_subs {
    // Standard 已由「自动选择」覆盖时不再单列，避免 fallback 重复测同一批
    if sub.tier == SubscriptionTier::Standard && !auto_select_group_proxies.is_empty() {
      continue;
    }
    if !fallback_group_proxies.contains(&sub.name) {
      fallback_group_proxies.push(sub.name.clone());
    }
  }

  // 节点选择：select 默认取列表第一项。把「故障转移」放首位，导入后即可跨组兜底（select 本身不会失败切换）
  let mut select_group_proxies = vec!["故障转移".to_string()];
  if !auto_select_group_proxies.is_empty() {
    select_group_proxies.push("自动选择".to_string());
  }
  for sub in &alive_subs {
    select_group_proxies.push(sub.name.clone());
  }
  select_group_proxies.push("DIRECT".to_string());

  let mut proxy_groups = vec![ProxyGroup {
    name: "节点选择".to_string(),
    r#type: "select".to_string(),
    proxies: select_group_proxies,
    other_fields: BTreeMap::new(),
  }];

  if !auto_select_group_proxies.is_empty() {
    proxy_groups.push(ProxyGroup {
      name: "自动选择".to_string(),
      r#type: "url-test".to_string(),
      proxies: auto_select_group_proxies,
      other_fields: url_test_fields(600, 200),
    });
  }

  proxy_groups.push(ProxyGroup {
    name: "故障转移".to_string(),
    r#type: "fallback".to_string(),
    proxies: fallback_group_proxies,
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
  });

  // LLM：优先手选 Premium；并提供「故障转移」作兜底选项
  let mut llm_proxies = premium_group_proxies;
  if !llm_proxies.iter().any(|p| p == "故障转移") {
    llm_proxies.push("故障转移".to_string());
  }
  if llm_proxies.is_empty() {
    llm_proxies.push("故障转移".to_string());
  }
  proxy_groups.push(ProxyGroup {
    name: "LLM".to_string(),
    r#type: "select".to_string(),
    proxies: llm_proxies,
    other_fields: BTreeMap::new(),
  });

  proxy_groups.push(ProxyGroup {
    name: "Steam".to_string(),
    r#type: "select".to_string(),
    proxies: vec!["DIRECT".to_string()],
    other_fields: BTreeMap::new(),
  });

  // 订阅信息：仅展示用 reject 节点，不进 url-test / fallback / 节点选择
  let mut info_nodes = Vec::new();
  let mut info_names = Vec::new();
  for sub in &subscriptions {
    let Some(traffic) = traffic_by_name.get(&sub.name) else {
      continue;
    };
    let name = format_traffic_label(&sub.name, traffic);
    println!("机场 [{}] 流量: {}", sub.name, name);
    info_names.push(name.clone());
    info_nodes.push(info_display_node(name));
  }
  if !info_names.is_empty() {
    // 独立 select：只给人看，不挂到节点选择 / 自动选择 / 故障转移
    proxy_groups.push(ProxyGroup {
      name: "订阅信息".to_string(),
      r#type: "select".to_string(),
      proxies: info_names,
      other_fields: BTreeMap::new(),
    });
  }

  for sub in &alive_subs {
    let proxies: Vec<String> = all_nodes
      .iter()
      .filter(|node| node.group_name == sub.name)
      .map(|node| node.name.clone())
      .collect();
    // alive_subs 已保证非空，此处再兜底一次
    if proxies.is_empty() {
      continue;
    }
    proxy_groups.push(ProxyGroup {
      name: sub.name.clone(),
      r#type: "url-test".to_string(),
      proxies,
      other_fields: url_test_fields(600, 200),
    });
  }

  let text = fs::read_to_string("./template.yaml").map_err(|e| {
    eprintln!("读取 template.yaml 失败: {}", e);
    e
  })?;
  let mut my_subscritption = serde_yaml::from_str::<MySubscritption>(&text).map_err(|e| {
    eprintln!("解析 template.yaml 失败: {}", e);
    e
  })?;
  let mut proxies_out = all_nodes;
  proxies_out.extend(info_nodes);
  my_subscritption.proxies = proxies_out;
  my_subscritption.proxy_groups = proxy_groups;
  let text = serde_yaml::to_string(&my_subscritption).map_err(|e| {
    eprintln!("序列化 Clash 配置失败: {}", e);
    e
  })?;
  Ok(text)
}

fn url_test_fields(interval: u64, tolerance: u64) -> BTreeMap<String, Value> {
  let mut map = BTreeMap::new();
  map.insert(
    "url".to_string(),
    serde_yaml::to_value("http://www.YouTube.com").unwrap(),
  );
  map.insert(
    "interval".to_string(),
    serde_yaml::to_value(interval).unwrap(),
  );
  map.insert(
    "tolerance".to_string(),
    serde_yaml::to_value(tolerance).unwrap(),
  );
  map
}

/// 将节点名改为 `{机场}-{地区}`（地区由原名识别）；同机场同地区冲突则追加序号。
fn uniquify_proxy_names(nodes: &mut [ProxyNode]) {
  let mut used: HashMap<String, usize> = HashMap::new();
  for node in nodes.iter_mut() {
    let region = detect_region(&node.name);
    let base = format!("{}-{}", node.group_name, region);
    let entry = used.entry(base.clone()).or_insert(0);
    *entry += 1;
    if *entry == 1 {
      node.name = base;
    } else {
      node.name = format!("{}-{}", base, *entry);
    }
  }
}

/// 按策略截取单个机场的存活节点。`limit == None` 时保留全部（仍会按延迟排序）。
fn select_subscription_nodes(
  mut nodes: Vec<ProxyNode>,
  limit: Option<usize>,
  mode: SelectMode,
  min_per_region: usize,
  max_per_region: Option<usize>,
) -> Vec<ProxyNode> {
  nodes.sort_by_key(|node| node.latency.unwrap_or(u64::MAX));

  let Some(limit) = limit else {
    return nodes;
  };
  if nodes.len() <= limit {
    return nodes;
  }

  match mode {
    SelectMode::LatencyTop => {
      nodes.truncate(limit);
      nodes
    }
    SelectMode::RegionDiverse => {
      select_region_diverse(nodes, limit, min_per_region.max(1), max_per_region)
    }
  }
}

/// 地区多样性截取：先按 `min_per_region` 给各区保底，再按「已取更少优先、其次延迟」轮询补满。
fn select_region_diverse(
  nodes: Vec<ProxyNode>,
  limit: usize,
  min_per_region: usize,
  max_per_region: Option<usize>,
) -> Vec<ProxyNode> {
  let mut buckets: HashMap<String, VecDeque<ProxyNode>> = HashMap::new();
  let mut region_order: Vec<String> = Vec::new();

  for node in nodes {
    let region = detect_region(&node.name).to_string();
    if !buckets.contains_key(&region) {
      region_order.push(region.clone());
    }
    buckets.entry(region).or_default().push_back(node);
  }

  // 有明确地区的桶按区内最优延迟排序；「其他」放最后，避免占满保底名额。
  region_order.sort_by(|a, b| {
    let a_other = a == "其他";
    let b_other = b == "其他";
    match (a_other, b_other) {
      (true, false) => std::cmp::Ordering::Greater,
      (false, true) => std::cmp::Ordering::Less,
      _ => {
        let la = buckets
          .get(a)
          .and_then(|q| q.front())
          .and_then(|n| n.latency)
          .unwrap_or(u64::MAX);
        let lb = buckets
          .get(b)
          .and_then(|q| q.front())
          .and_then(|n| n.latency)
          .unwrap_or(u64::MAX);
        la.cmp(&lb).then_with(|| a.cmp(b))
      }
    }
  });

  let mut selected: Vec<ProxyNode> = Vec::with_capacity(limit);
  let mut taken: HashMap<String, usize> = HashMap::new();

  for _ in 0..min_per_region {
    if selected.len() >= limit {
      break;
    }
    let mut progressed = false;
    let candidates: Vec<String> = region_order
      .iter()
      .filter(|region| {
        region_can_take(
          region,
          &buckets,
          &taken,
          selected.len(),
          limit,
          max_per_region,
        )
      })
      .cloned()
      .collect();
    for region in candidates {
      if selected.len() >= limit {
        break;
      }
      if !region_can_take(
        &region,
        &buckets,
        &taken,
        selected.len(),
        limit,
        max_per_region,
      ) {
        continue;
      }
      let Some(node) = buckets.get_mut(&region).and_then(|q| q.pop_front()) else {
        continue;
      };
      *taken.entry(region).or_insert(0) += 1;
      selected.push(node);
      progressed = true;
    }
    if !progressed {
      break;
    }
  }

  while selected.len() < limit {
    let mut best: Option<(usize, u64, String)> = None;
    for region in &region_order {
      if !region_can_take(
        region,
        &buckets,
        &taken,
        selected.len(),
        limit,
        max_per_region,
      ) {
        continue;
      }
      let count = taken.get(region).copied().unwrap_or(0);
      let lat = buckets
        .get(region)
        .and_then(|q| q.front())
        .and_then(|n| n.latency)
        .unwrap_or(u64::MAX);
      let better = match &best {
        None => true,
        Some((best_count, best_lat, best_region)) => {
          (count, lat, region.as_str()) < (*best_count, *best_lat, best_region.as_str())
        }
      };
      if better {
        best = Some((count, lat, region.clone()));
      }
    }

    let Some((_, _, region)) = best else {
      break;
    };
    let Some(queue) = buckets.get_mut(&region) else {
      break;
    };
    let Some(node) = queue.pop_front() else {
      break;
    };
    *taken.entry(region).or_insert(0) += 1;
    selected.push(node);
  }

  selected
}

fn region_can_take(
  region: &str,
  buckets: &HashMap<String, VecDeque<ProxyNode>>,
  taken: &HashMap<String, usize>,
  selected_len: usize,
  limit: usize,
  max_per_region: Option<usize>,
) -> bool {
  if selected_len >= limit {
    return false;
  }
  let count = taken.get(region).copied().unwrap_or(0);
  if let Some(max) = max_per_region
    && count >= max
  {
    return false;
  }
  buckets.get(region).is_some_and(|q| !q.is_empty())
}

/// 从节点名识别地区（机场命名约定为主信号）。
fn detect_region(name: &str) -> &'static str {
  let lower = name.to_lowercase();

  // 短语 / 中文优先（避免短码误匹配）
  let phrase_rules: &[(&[&str], &str)] = &[
    (&["香港", "hong kong", "hongkong"], "香港"),
    (&["澳门", "澳門", "macau", "macao"], "澳门"),
    (&["台湾", "台灣", "臺灣", "taipei", "taiwan"], "台湾"),
    (&["新加坡", "singapore", "狮城", "獅城"], "新加坡"),
    (
      &[
        "日本",
        "东京",
        "東京",
        "大阪",
        "名古屋",
        "japan",
        "tokyo",
        "osaka",
      ],
      "日本",
    ),
    (&["韩国", "韓國", "首尔", "首爾", "korea", "seoul"], "韩国"),
    (
      &[
        "美国",
        "美國",
        "usa",
        "united states",
        "los angeles",
        "san jose",
        "san francisco",
        "seattle",
        "chicago",
        "dallas",
        "miami",
        "ashburn",
        "buffalo",
        "new york",
        "las vegas",
      ],
      "美国",
    ),
    (
      &["英国", "英國", "london", "united kingdom", "britain"],
      "英国",
    ),
    (&["德国", "德國", "frankfurt", "germany", "berlin"], "德国"),
    (&["法国", "法國", "paris", "france"], "法国"),
    (&["荷兰", "荷蘭", "amsterdam", "netherlands"], "荷兰"),
    (
      &["加拿大", "canada", "toronto", "vancouver", "montreal"],
      "加拿大",
    ),
    (
      &["澳大利亚", "澳洲", "australia", "sydney", "melbourne"],
      "澳大利亚",
    ),
    (&["俄罗斯", "俄羅斯", "russia", "moscow"], "俄罗斯"),
    (&["印度", "india", "mumbai"], "印度"),
    (&["土耳其", "turkey", "istanbul"], "土耳其"),
    (&["阿根廷", "argentina", "buenos"], "阿根廷"),
    (&["巴西", "brazil", "sao paulo"], "巴西"),
    (&["菲律宾", "philippines", "manila"], "菲律宾"),
    (&["泰国", "泰國", "thailand", "bangkok"], "泰国"),
    (&["马来西亚", "馬來西亞", "malaysia", "kuala"], "马来西亚"),
    (&["越南", "vietnam", "hanoi", "saigon"], "越南"),
    (&["印尼", "indonesia", "jakarta"], "印尼"),
    (&["尼日利亚", "nigeria", "lagos"], "尼日利亚"),
    (&["迪拜", "dubai", "uae"], "阿联酋"),
  ];

  for (needles, region) in phrase_rules {
    for needle in *needles {
      if needle.is_ascii() {
        if lower.contains(needle) {
          return region;
        }
      } else if name.contains(needle) {
        return region;
      }
    }
  }

  // 短地区码按非字母数字分词，降低 `US` 误伤
  let code_rules: &[(&[&str], &str)] = &[
    (&["HK"], "香港"),
    (&["MO"], "澳门"),
    (&["TW"], "台湾"),
    (&["SG"], "新加坡"),
    (&["JP", "TYO", "OSA"], "日本"),
    (&["KR", "ICN"], "韩国"),
    (
      &[
        "US", "USA", "LAX", "SJC", "SFO", "SEA", "ORD", "DFW", "MIA", "IAD", "NYC",
      ],
      "美国",
    ),
    (&["GB", "UK", "LON"], "英国"),
    (&["DE", "FRA"], "德国"),
    (&["FR", "PAR"], "法国"),
    (&["NL", "AMS"], "荷兰"),
    (&["CA", "YVR", "YYZ"], "加拿大"),
    (&["AU", "SYD", "MEL"], "澳大利亚"),
    (&["RU"], "俄罗斯"),
    (&["IN"], "印度"),
    (&["TR"], "土耳其"),
    (&["AR"], "阿根廷"),
    (&["BR"], "巴西"),
    (&["PH"], "菲律宾"),
    (&["TH"], "泰国"),
    (&["MY"], "马来西亚"),
    (&["VN"], "越南"),
    (&["ID"], "印尼"),
  ];

  for (codes, region) in code_rules {
    for code in *codes {
      if name_has_region_code(name, code) {
        return region;
      }
    }
  }

  "其他"
}

fn name_has_region_code(name: &str, code: &str) -> bool {
  let code = code.to_ascii_uppercase();
  name
    .split(|c: char| !c.is_ascii_alphanumeric())
    .filter(|t| !t.is_empty())
    .any(|token| token.eq_ignore_ascii_case(&code))
}

async fn fetch_and_decode_sub(sub: &Subscription) -> FetchResult {
  let empty = || FetchResult {
    name: sub.name.clone(),
    nodes: Vec::new(),
    traffic: None,
  };

  let client = reqwest::Client::new();
  let resp = match client
    .get(sub.url.clone())
    .header(USER_AGENT, "clash-verge/v2.5.1")
    .send()
    .await
  {
    Ok(resp) => resp,
    Err(e) => {
      eprintln!("机场 [{}] 拉取失败: {}", sub.name, e);
      return empty();
    }
  };

  if !resp.status().is_success() {
    eprintln!("机场 [{}] HTTP 状态异常: {}", sub.name, resp.status());
    return empty();
  }

  let traffic = resp
    .headers()
    .get("subscription-userinfo")
    .and_then(|v| v.to_str().ok())
    .and_then(parse_subscription_userinfo);

  let text = match resp.text().await {
    Ok(text) => text,
    Err(e) => {
      eprintln!("机场 [{}] 读取响应失败: {}", sub.name, e);
      return FetchResult {
        name: sub.name.clone(),
        nodes: Vec::new(),
        traffic,
      };
    }
  };

  let proxy_nodes = match extract_proxies_from_yaml(&text) {
    Ok(nodes) => nodes,
    Err(e) => {
      eprintln!("机场 [{}] 解析 YAML 失败: {}", sub.name, e);
      return FetchResult {
        name: sub.name.clone(),
        nodes: Vec::new(),
        traffic,
      };
    }
  };

  let mut result_nodes = Vec::with_capacity(proxy_nodes.len());
  for mut node in proxy_nodes {
    node.source_tier = sub.tier.clone();
    node.group_name = sub.name.clone();
    result_nodes.push(node);
  }

  if result_nodes.is_empty() {
    eprintln!("机场 [{}] 未解析到任何节点", sub.name);
  } else {
    println!("机场 [{}] 拉取到 {} 个节点", sub.name, result_nodes.len());
  }

  FetchResult {
    name: sub.name.clone(),
    nodes: result_nodes,
    traffic,
  }
}

/// 解析 `upload=; download=; total=; expire=`（分号分隔，空白可有可无）。
fn parse_subscription_userinfo(raw: &str) -> Option<SubscriptionTraffic> {
  let mut traffic = SubscriptionTraffic::default();
  let mut any = false;

  for part in raw.split(';') {
    let part = part.trim();
    if part.is_empty() {
      continue;
    }
    let Some((key, value)) = part.split_once('=') else {
      continue;
    };
    let key = key.trim();
    let value = value.trim();
    match key {
      "upload" => {
        if let Ok(v) = value.parse::<u64>() {
          traffic.upload = Some(v);
          any = true;
        }
      }
      "download" => {
        if let Ok(v) = value.parse::<u64>() {
          traffic.download = Some(v);
          any = true;
        }
      }
      "total" => {
        if let Ok(v) = value.parse::<u64>() {
          traffic.total = Some(v);
          any = true;
        }
      }
      "expire" => {
        if let Ok(v) = value.parse::<i64>() {
          traffic.expire = Some(v);
          any = true;
        }
      }
      _ => {}
    }
  }

  any.then_some(traffic)
}

fn format_bytes_gib(bytes: u64) -> String {
  const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
  format!("{:.2}GB", bytes as f64 / GIB)
}

fn format_expire(ts: i64) -> String {
  // 避免引入 chrono：按 UTC 粗格式化 YYYY-MM-DD
  if ts <= 0 {
    return "永久".to_string();
  }
  // days from Unix epoch
  let days = ts.div_euclid(86_400);
  // civil_from_days (Howard Hinnant) — UTC 日期
  let z = days + 719_468;
  let era = z.div_euclid(146_097);
  let doe = (z - era * 146_097) as u64;
  let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
  let y = yoe as i64 + era * 400;
  let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  let mp = (5 * doy + 2) / 153;
  let d = doy - (153 * mp + 2) / 5 + 1;
  let m = if mp < 10 { mp + 3 } else { mp - 9 };
  let y = if m <= 2 { y + 1 } else { y };
  format!("{:04}-{:02}-{:02}", y, m, d)
}

fn format_traffic_label(airport: &str, t: &SubscriptionTraffic) -> String {
  let used = t
    .upload
    .unwrap_or(0)
    .saturating_add(t.download.unwrap_or(0));
  let usage = match t.total {
    Some(total) if total > 0 => {
      let pct = (used as f64 / total as f64 * 100.0).clamp(0.0, 999.0);
      format!(
        "{}/{} ({:.0}%)",
        format_bytes_gib(used),
        format_bytes_gib(total),
        pct
      )
    }
    Some(_) => format!("{}/?", format_bytes_gib(used)),
    None => {
      if t.upload.is_some() || t.download.is_some() {
        format!("{} used", format_bytes_gib(used))
      } else {
        "流量未知".to_string()
      }
    }
  };

  match t.expire {
    Some(ts) => format!("{airport} | {usage} | 到期 {}", format_expire(ts)),
    None => format!("{airport} | {usage}"),
  }
}

/// 展示用占位节点。仅出现在「订阅信息」select 中，不进测速/自动组。
/// 用本地 socks5 占位：多数内核不接受 proxies 里写 type=reject。
fn info_display_node(name: String) -> ProxyNode {
  ProxyNode {
    name,
    r#type: "socks5".to_string(),
    server: "127.0.0.1".to_string(),
    port: 1,
    source_tier: SubscriptionTier::Backup,
    group_name: "订阅信息".to_string(),
    latency: None,
    other_fields: BTreeMap::new(),
  }
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

fn extract_proxies_from_yaml(text: &str) -> Result<Vec<ProxyNode>, serde_yaml::Error> {
  let clash_config = serde_yaml::from_str::<ClashConfig>(text)?;
  Ok(clash_config.proxies)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn node(name: &str, latency: u64) -> ProxyNode {
    ProxyNode {
      name: name.to_string(),
      r#type: "ss".to_string(),
      server: "127.0.0.1".to_string(),
      port: 1,
      source_tier: SubscriptionTier::Standard,
      group_name: "test".to_string(),
      latency: Some(latency),
      other_fields: BTreeMap::new(),
    }
  }

  #[test]
  fn detect_region_common_names() {
    assert_eq!(detect_region("香港 IEPL 01"), "香港");
    assert_eq!(detect_region("JP-Tokyo-01"), "日本");
    assert_eq!(detect_region("美国洛杉矶"), "美国");
    assert_eq!(detect_region("US-LAX-2"), "美国");
    assert_eq!(detect_region("新加坡 BGP"), "新加坡");
    assert_eq!(detect_region("套餐剩余"), "其他");
  }

  #[test]
  fn region_diverse_keeps_us_when_asia_faster() {
    let nodes = vec![
      node("香港01", 20),
      node("香港02", 25),
      node("香港03", 30),
      node("日本01", 40),
      node("日本02", 45),
      node("韩国01", 50),
      node("韩国02", 55),
      node("美国01", 180),
      node("美国02", 200),
      node("新加坡01", 60),
    ];

    let selected = select_subscription_nodes(nodes, Some(5), SelectMode::RegionDiverse, 1, Some(2));
    let regions: Vec<_> = selected.iter().map(|n| detect_region(&n.name)).collect();

    assert_eq!(selected.len(), 5);
    assert!(regions.contains(&"美国"), "应保留美区，实际: {:?}", regions);
    assert!(regions.contains(&"香港"));
    // 不应被港日韩低延迟占满成单一地区
    let hk = regions.iter().filter(|r| **r == "香港").count();
    assert!(hk <= 2);
  }

  #[test]
  fn latency_top_still_prefers_lowest_rtt() {
    let nodes = vec![
      node("香港01", 20),
      node("香港02", 25),
      node("美国01", 180),
      node("日本01", 40),
    ];
    let selected = select_subscription_nodes(nodes, Some(2), SelectMode::LatencyTop, 1, None);
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].name, "香港01");
    assert_eq!(selected[1].name, "香港02");
  }

  #[test]
  fn max_per_region_caps_bucket() {
    let nodes = vec![
      node("香港01", 10),
      node("香港02", 11),
      node("香港03", 12),
      node("美国01", 100),
      node("日本01", 40),
    ];
    let selected = select_subscription_nodes(nodes, Some(4), SelectMode::RegionDiverse, 1, Some(1));
    let hk = selected
      .iter()
      .filter(|n| detect_region(&n.name) == "香港")
      .count();
    assert_eq!(hk, 1);
    assert_eq!(selected.len(), 3); // 只有 3 个地区
  }

  #[test]
  fn parse_subscription_userinfo_basic() {
    let t = parse_subscription_userinfo(
      "upload=1073741824; download=2147483648; total=10737418240; expire=1893456000",
    )
    .expect("should parse");
    assert_eq!(t.upload, Some(1073741824));
    assert_eq!(t.download, Some(2147483648));
    assert_eq!(t.total, Some(10737418240));
    assert_eq!(t.expire, Some(1893456000));
    assert_eq!(format_expire(1893456000), "2030-01-01");

    let label = format_traffic_label("provider-a", &t);
    assert!(label.contains("provider-a"));
    assert!(label.contains("到期"));
    assert!(label.contains('%'));

    assert!(parse_subscription_userinfo("").is_none());
    assert!(parse_subscription_userinfo("foo=bar").is_none());
  }

  #[test]
  fn uniquify_proxy_names_uses_airport_and_region() {
    let mut nodes = vec![
      ProxyNode {
        name: "香港 IEPL 01".to_string(),
        r#type: "ss".to_string(),
        server: "1.1.1.1".to_string(),
        port: 1,
        source_tier: SubscriptionTier::Standard,
        group_name: "大米".to_string(),
        latency: Some(10),
        other_fields: BTreeMap::new(),
      },
      ProxyNode {
        name: "US-LAX-2".to_string(),
        r#type: "ss".to_string(),
        server: "2.2.2.2".to_string(),
        port: 1,
        source_tier: SubscriptionTier::Standard,
        group_name: "飞鸟云".to_string(),
        latency: Some(20),
        other_fields: BTreeMap::new(),
      },
      ProxyNode {
        name: "香港 BGP 02".to_string(),
        r#type: "ss".to_string(),
        server: "3.3.3.3".to_string(),
        port: 1,
        source_tier: SubscriptionTier::Standard,
        group_name: "大米".to_string(),
        latency: Some(30),
        other_fields: BTreeMap::new(),
      },
      ProxyNode {
        name: "神秘线路".to_string(),
        r#type: "ss".to_string(),
        server: "4.4.4.4".to_string(),
        port: 1,
        source_tier: SubscriptionTier::Standard,
        group_name: "大米".to_string(),
        latency: Some(40),
        other_fields: BTreeMap::new(),
      },
    ];
    uniquify_proxy_names(&mut nodes);
    assert_eq!(nodes[0].name, "大米-香港");
    assert_eq!(nodes[1].name, "飞鸟云-美国");
    assert_eq!(nodes[2].name, "大米-香港-2");
    assert_eq!(nodes[3].name, "大米-其他");
  }
}
