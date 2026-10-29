# tag-search-bench

在 TiDB 上按 tag 检索用户（"人群圈选"）的几种方案对比。

起源于一个真实的业务问题：300 万用户、2400 万条 user-tag 关联，按多个 tag 取交集检索，TiDB 上单条 SQL 要十几秒。

## 方案

| 方案 | 思路 | 状态 |
|---|---|---|
| `baseline` | 一条 `JOIN + GROUP BY + HAVING` SQL，全部交给 TiDB | ✅ |
| `sharded` | 按 UUID 前缀把用户切成 N 段，用 worker pool 并发查询，在 Go 里汇总（Scatter-Gather） | ✅ |
| `bitmap` | 每个 tag 一个 Roaring Bitmap 放在内存里，查询就是位运算 AND | ✅ |

## 各方案优劣

### baseline

优点：
- 实现最简单，只有一条 SQL
- 数据实时，写入后马上就能查到
- 不占应用内存，也不需要额外的组件

缺点：
- Join 和聚合都集中在单个 TiDB Server 上执行，数据量越大越慢，增加 TiKV 节点也改善不了
- 热门 tag 组合的中间结果很大（tags 1,2,3 时 Join 出 496 万行），TiDB Server 的内存压力大

### sharded

优点：
- 不改表结构，也不加组件，只改应用层的查询方式
- 数据实时
- 一个大的 Join 和聚合被拆成多个小查询；部署了多个 TiDB Server 时，计算可以分散到多台机器上
- 实测比 baseline 快 4～5 倍

缺点：
- 分片数需要调：太少则并发用不满，太多则每次查询的固定开销占主导；最优值会随数据量和查询条件变化
- 并发查询会在短时间内给数据库很大压力，访问量高时会挤占其他业务的资源
- 依赖分片键分布均匀：随机的 UUID v4 没问题，UUID v7 和自增 ID 按前缀切会严重倾斜
- 排序、分页、去重计数等跨分片的操作需要在应用层归并，深分页尤其麻烦
- 各分片是独立的 SQL，读的不是同一个快照，查询期间有写入时结果可能不一致（可以给所有分片指定同一个读时间戳来解决）

### bitmap

优点：
- 查询极快：位运算只要几十到两百微秒，基本不受数据量影响
- 查询完全不访问数据库
- 内存占用小：300 万用户、2000 个 tag 的位图一共 37.5 MB
- 天然支持 AND / OR / NOT 的任意组合

缺点：
- 索引只是一份快照，需要增量同步（比如用 TiCDC 订阅变更）或者定期重建，数据有延迟
- 启动时要先建索引（实测 3.9 秒，会随数据量增长），可以考虑把位图持久化
- 部署多个实例时，每个实例都持有一份索引，需要保证彼此一致
- 需要连续的整数 ID（这里是 `users.seq`），用户删除后会在位图里留下空洞
- 只适合"有没有某个 tag"这类等值条件；范围条件（比如年龄区间）要先分桶，模糊匹配不适用
- 返回的结果集很大时，瓶颈会转移到把 ID 转回 UUID 以及传输结果上

## baseline 为什么慢

- TiKV 按主键范围自动把数据切成 Region，单表的过滤和聚合可以下推（Coprocessor），但 **Join 不下推**，大量中间结果被拉到一台 TiDB Server 上做 Join
- 走二级索引时如果需要回表（`IndexLookUp`），命中行数多时 RPC 数量会非常大（本数据集有 `(tag_id, user_id)` 联合索引，不需要回表）

## 快速开始

### 1. 启动 TiDB（WSL / Linux / macOS）

TiUP 不支持 Windows，Windows 上要在 WSL 里跑：

```bash
bash scripts/playground.sh
```

启动后：
- SQL：`127.0.0.1:4000`（用户 `root`，无密码）
- Dashboard：`http://127.0.0.1:2379/dashboard`
- Grafana：启动日志里会打印地址

用了 `--tag tagbench`，数据保存在 `~/.tiup/data/tagbench`，重启后不会丢。

### 2. 生成数据

```bash
# 先用 30 万用户把流程跑通
go run ./cmd/gen -users 300000 -drop

# 原始规模：300 万用户，平均 8 个 tag/人，约 2400 万行
go run ./cmd/gen -users 3000000 -drop
```

tag 按 Zipf 分布生成（少数 tag 极热门，多数很冷门），更接近真实数据。tag 1 最热门。

### 3. 跑测试

```bash
go run ./cmd/bench -strategy baseline -tags 3,17,120
go run ./cmd/bench -strategy sharded -shards 16 -workers 16 -verify

# 分片数扫描：画出耗时随分片数变化的 U 形曲线
go run ./cmd/bench -sweep 4,8,16,32,64,128,256,512,1024 -workers 16 -out results/sweep.csv
```

`-verify` 会先跑一遍 baseline，检查结果是否一致。

## 实验计划

- [x] baseline：`EXPLAIN ANALYZE` 定位瓶颈（`-explain`）
- [x] sharded：实现 Scatter-Gather
- [x] 分片数扫描：找到最优点，解释两边为什么变慢（固定开销 vs. 长尾）
- [ ] 去掉 `idx_tag_user`，模拟回表场景
- [ ] 分片边界对齐 Region（`SHOW TABLE users REGIONS`）是否更快
- [ ] worker 数扫描：并发高了以后数据库端会不会成为瓶颈
- [ ] 热门 tag 和冷门 tag 组合下的表现差异
- [x] bitmap：建索引耗时、内存占用、查询耗时
- [ ] bitmap：通过 TiCDC 做增量更新
- [ ] 对比：TiFlash MPP（`--tiflash 1`）

## 结果

环境：tiup playground v8.5.8（1 PD + 3 TiKV + 1 TiDB，单机 WSL2），300 万用户 / 2400 万 user_tags，16 个 worker。

### 三种方案对比（p50，结果都已用 `-verify` 和 baseline 核对一致）

| tag 组合 | 命中用户数 | baseline | sharded (16 片) | bitmap | 其中 bitmap AND 本身 |
|---|---|---|---|---|---|
| 1, 2, 3（热门） | 477,556 | 1.78s | 0.43s | 21ms | 0.13ms |
| 3, 17, 120（冷门） | 641 | 0.80s | 0.15s | <1ms | 0.05ms |
| 1, 2 | 1,206,050 | 1.51s | | 32ms | 0.06ms |
| 1 | 2,296,094 | 1.07s | | 60ms | 0.05ms |
| 1, 2 + region 5 | 60,254 | 0.60s | | 5ms | 0.19ms |

bitmap 的查询时间几乎全花在把命中的 seq 转回 UUID 字符串上，耗时和结果集大小成正比；位运算本身只要几十到两百微秒。

### bitmap 的代价

| 项目 | 数值 |
|---|---|
| 建索引耗时 | 3.9s（64 段并行从 TiDB 读取） |
| 2000 个 tag + 20 个 region 的位图 | 37.5 MB |
| seq → UUID 映射表 | 45.8 MB（每个 UUID 存 16 字节，不存 36 字节的字符串） |

### 分片数扫描（tags 1,2,3，16 worker）

| 分片数 | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256 | 512 | 1024 | 2048 | 4096 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p50 (ms) | 1682 | 1064 | 850 | 491 | **430** | 518 | 437 | 476 | 718 | 1014 | 1278 | 1518 | 1982 |

原始数据见 `results/`。

- **左侧**：分片数小于 worker 数时并发用不满，耗时随分片数增加快速下降
- **谷底**：16 到 128 片，大约就是 worker 数的 1～8 倍
- **右侧**：从 256 片开始，每片只剩一万多个用户，每次查询的固定开销（网络往返、SQL 解析和优化、下发 Coprocessor 任务）成了主要成本，总耗时反而上升
- 冷门 tag 的结果集小，每片实际工作量更少，所以拐点出现得更早（16 片 153ms，64 片就回升到 200ms）

### 执行计划（baseline，tags 1,2,3）

```
HashAgg      1.61s   ← TiDB Server 上 GROUP BY 275 万行
└─MergeJoin  1.27s   ← TiDB Server 上 Join 496 万行
  ├─IndexRangeScan (tikv)  434ms, 147 cop tasks
  └─TableFullScan  (tikv)  users 300 万行
```

TiKV 的扫描是分布式并行的，只花了 0.4s；大部分时间花在单个 TiDB Server 上做 Join 和聚合。

## 注意

playground 所有组件跑在一台机器上，绝对数值不代表生产环境，只用于方案之间的相对比较。

TiKV 默认按整机内存来定缓存大小（block cache 占 45%）。单机起 3 个 TiKV 时，3 个实例加起来要的内存会超过机器实际有的，导致机器开始用 swap、最后卡死（实测在 24GB 内存的 WSL 上灌数据到 62% 时卡死）。所以 `scripts/tikv.toml` 给每个 TiKV 限制了内存。
