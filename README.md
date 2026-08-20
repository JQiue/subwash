# Subwash

用 Rust 写的轻量 Clash 订阅清洗工具。

并行拉取多个订阅，过滤垃圾节点，ICMP 测速后按策略截取，改名并合并进本地模板，通过 HTTP 输出。

```text
订阅源 ──► 拉取 ──► 过滤 ──► 测速 ──► 截取 ──► 改名 ──► 合并模板 ──► GET /subscribe
```

## 功能

- 多订阅并行拉取
- 关键词过滤 + ICMP 测速
- 每机场 `max_nodes` 限额
- 默认 `region_diverse`（按地区保底），可选 `latency_top`
- 节点名统一为 `{机场}-{地区}`（冲突加 `-2`、`-3`…）
- 只覆盖模板的 `proxies` / `proxy-groups`，DNS 和 rules 不动
- 成功结果缓存 600s；失败返回 500 且不缓存

## 依赖文件

工作目录下需要：

| 文件 | 作用 |
| --- | --- |
| `config.yaml` | 订阅与清洗参数 |
| `template.yaml` | Clash 基底配置 |

## 快速开始

```bash
cargo run
# cargo run --release
```

```text
GET http://127.0.0.1:5000/subscribe
```

监听地址优先级：`SUBWASH_LISTEN` > `config.listen` > `127.0.0.1:5000`

```powershell
$env:SUBWASH_LISTEN="0.0.0.0:5000"; cargo run
```

改配置或代码后重启进程，或等缓存过期。

## 配置

```yaml
config:
  speed_test: 1500              # ICMP 超时（ms），失败节点丢弃
  exclude_keyword:              # 原名包含任一关键词则剔除（测速前）
    - 套餐
    - 流量
    - 官网
    - 电报
    - 失联
  select_mode: region_diverse   # region_diverse | latency_top
  min_per_region: 1             # 仅 region_diverse
  max_per_region: 2             # 仅 region_diverse；不写 = 不限制
  listen: "127.0.0.1:5000"      # 可被 SUBWASH_LISTEN 覆盖

subscriptions:
  - name: provider-a            # 策略组名 + 节点名前缀
    tier: Premium               # Premium | Standard | Backup
    url: "https://example.com/sub?token=xxx"
    max_nodes: 8                # 可选；不写 = 保留全部存活节点
    # select_mode / min_per_region / max_per_region 可覆盖全局

  - name: provider-b
    tier: Standard
    url: "https://example.com/sub?token=yyy"
    max_nodes: 8
```

### `config`

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `speed_test` | 必填 | ICMP 超时（ms） |
| `exclude_keyword` | — | 匹配节点**原名** |
| `select_mode` | `region_diverse` | 见下；可被订阅项覆盖 |
| `min_per_region` | `1` | 仅 `region_diverse` |
| `max_per_region` | 不限制 | 仅 `region_diverse` |
| `listen` | `127.0.0.1:5000` | HTTP 监听地址 |

### `select_mode`

| 模式 | 行为 |
| --- | --- |
| `region_diverse` | 按节点名分地区 → 各区 `min_per_region` 保底 → 再按「已取更少、延迟更低」补满 `max_nodes`。识别失败归 `其他`（保底优先级最低） |
| `latency_top` | 全局按延迟升序截断 |

### `subscriptions[]`

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `name` | 是 | 策略组名、节点名前缀 |
| `url` | 是 | Clash YAML 订阅（含 `proxies`） |
| `tier` | 是 | 决定进入哪些内置策略组 |
| `max_nodes` | 否 | 测速后最多保留几个 |
| `select_mode` | 否 | 覆盖全局 |
| `min_per_region` | 否 | 覆盖全局 |
| `max_per_region` | 否 | 覆盖全局 |

单个订阅挂了会跳过；**全部**无可用节点时 `/subscribe` 返回 500。

## 策略组

代码里写死的分组（分流规则仍在 `template.yaml`）：

| 组名 | 类型 | 成员 |
| --- | --- | --- |
| 节点选择 | `select` | `故障转移` → `自动选择` → 各机场 → `DIRECT`（**第一项为默认**） |
| 自动选择 | `url-test` | 存活的 `Standard` 机场 |
| 故障转移 | `fallback` | `自动选择` → `Premium` 机场 → 其余机场 |
| LLM | `select` | `Premium` 机场 + `故障转移` |
| Steam | `select` | 目前仅 `DIRECT` |
| _机场名_ | `url-test` | 该机场截取后的节点 |

- `select` 不会因连接失败自动换下一个；跨机场兜底要选中 **故障转移**（默认已是第一项）
- `fallback` 按健康检查顺序切换，不是每次请求扫全部叶子节点
- 无存活节点的机场不生成空组，避免 Clash 校验失败
- `Backup` 枚举有，组装时基本没用

## 节点命名

1. 用**原名**识别地区（`detect_region`）
2. 改成 `{机场}-{地区}`
3. 冲突：`provider-a-香港`、`provider-a-香港-2`…

## 注意

- Windows 上 ICMP 常常要管理员权限，否则存活节点会很少或为 0
- 订阅 token 不要提交进仓库或贴到 issue
- 拉取订阅使用固定的 Clash 类 UA
- 必须在含 `config.yaml` / `template.yaml` 的目录启动

## 开发

```bash
cargo check
cargo test
cargo run
```

| 路径 | 职责 |
| --- | --- |
| `src/lib.rs` | 清洗主流程 `get_subscription` |
| `src/main.rs` | HTTP + 600s 缓存 |
