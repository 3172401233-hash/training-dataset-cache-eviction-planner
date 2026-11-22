# training-dataset-cache-eviction-planner

规划训练样本缓存淘汰、epoch 和局部性。

训练样本按随机 shuffle 顺序读取时，固定 LRU 会让下一个 epoch 的局部性完全
消失，缓存不足时 GPU 在数据加载上反复等待。本规划器接收一条**采样轨迹**
（sampling trace），在共享缓存上演进它，输出缓存集合、预取批次和预计停顿。

## 输入：采样轨迹

输入是一个 JSON 对象，描述样本大小、shuffle 计划、epoch、worker、共享样本、
缓存容量和本地磁盘带宽（完整示例见 `examples/trace.json`）：

```json
{
  "cacheCapacityBytes": 30,
  "diskBandwidthBytesPerSecond": 10,
  "secondsPerStep": 2,
  "prefetchWindow": 8,
  "prefetchBatchBytes": 1000000,
  "sampleSizes": { "s0": 10, "s1": 10 },
  "sharedSamples": ["s1"],
  "workers": [
    { "id": "worker-0", "epochs": [["s0", "s1"], ["s1", "s0"]] }
  ]
}
```

- `sampleSizes`：每个样本的字节数（必需）。
- `workers[].epochs`：每个 worker 每个 epoch 的洗牌后访问顺序，即采样轨迹
  本体（必需）。epoch 之间可以不同长、可以重复采样（动态采样）。
- `sharedSamples`：被多个 worker 共享的样本 ID。
- `cacheCapacityBytes` / `diskBandwidthBytesPerSecond`：缓存容量与本地磁盘
  带宽（必需）。
- `secondsPerStep`：GPU 消费一个样本的步长时间，默认 1。
- `prefetchWindow`：预取向前看的访问数，0 表示关闭预取，默认 32。
- `prefetchBatchBytes`：单个预取批次的最大字节数，默认不限。

同一 step 内按 worker 顺序交错成全局访问流；worker 轨迹长度不齐时，短轨迹
的 worker 在尾部空闲。

## 策略

- **淘汰**：Belady（最远下次使用）。同一 epoch 内还会被复用的热点样本下次
  使用更近，天然被优先保留；不再使用的样本最先出局。
- **共享样本保护**：共享样本只要还有任何 worker 的未来使用就不可被淘汰，
  因此不会因为某个 worker 切换 epoch 而被误删；仅当所有候选都受保护时才
  退化淘汰下次使用最远的受保护样本，避免缓存无法腾出空间。
- **预取**：在预取窗口内按访问顺序扫描未缓存的未来样本，只要磁盘队列能在
  其使用时间之前完成传输就排入预取批次。窗口跨越 epoch 边界，下一 epoch
  的可预测局部性会被提前准备。
- **停顿**：miss 时按 `样本大小 / 磁盘带宽` 排队占用磁盘，GPU 等待时间为
  `完成时刻 - 请求时刻`，预取在途的 miss 只等待剩余传输时间。

## 输出

`planCache(trace)` 返回：

- `finalCache` / `cacheSnapshots`：最终缓存集合与每个 (worker, epoch) 起点
  的缓存快照。
- `prefetchBatches`：预取批次（step、样本列表、字节数、完成时刻）。
- `totalStallSeconds` / `stallEvents`：预计总停顿与逐次停顿事件。
- `hits` / `misses` / `hitRate` / `epochs`：命中统计与每 (worker, epoch)
  的细分。
- `evictions` / `uncacheableSamples`：淘汰记录与超过缓存容量的样本。

## 使用

```bash
node bin/cli.js examples/trace.json   # 打印完整规划 JSON
npm test                              # node --test
```

```js
import { planCache } from './src/planner.js';
const plan = planCache(trace);
```

测试覆盖固定 shuffle、动态采样、共享样本保护和容量不足四种场景
（`test/planner.test.js`）。
