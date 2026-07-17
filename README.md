# Subwash

`subwash` 是一个用 Rust 编写的超轻量、极速 Clash 订阅节点清洗工具。

它能自动拉取多个机场订阅，清理节点，并根据配置保留每个机场延迟最低的活跃节点，最后聚合生成 Clash 配置文件。

## Feature

- **保留模板格式**：**100% 保留**原本模板文件中的 DNS、Rules 策略组顺序。
- **额度精确控制**：支持在配置文件中为每个机场单独配置 `max_nodes`，只保留延迟最低的前 `N` 个节点，告别大机场“节点霸屏”。

## 工作原理

```plain
[订阅源 A] ──┐
[订阅源 B] ──┼─► [ 1. 拉取并解析 ] ─► [ 2. 测速 ] ─► [ 3. 排序并按额度截取 ]
[订阅源 C] ──┘                                                      │
                                                                    ▼
[ 完美保留顺序的 clash_config.yaml ] ◄── [ 5. 写入本地模板 ] ◄── [ 4. 剔除所有死节点 ]
```

## 快速开始

在程序同级目录下创建 `config.yaml`：

```yaml
config:
  speed_test: 1500   # 保留 1500ms 以内的节点

# 订阅机场配置
subscriptions:
  - name: "订阅一"
    tier: "Premium"
    url: "https://example1.com/api/v1/client/subscribe?token=example1"
    max_nodes: 5  # 该订阅保留延迟最低的 5 个节点
  - name: "订阅二"
    tier: "Standard"
    url: "https://example2.com/api/v1/client/subscribe?token=example2"
    max_nodes: 8  # 该订阅保留延迟最低的 10 个节点
```

同时，准备好 Clash 基础模板文件 `template.yaml`
