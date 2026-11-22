// 训练样本缓存规划器。
//
// 输入为一条采样轨迹（sampling trace）：每个 worker 按 epoch 给出洗牌后的
// 样本访问顺序。规划器在共享缓存上演进这条轨迹，使用 Belady（最远下次使用）
// 淘汰策略保留同一 epoch 内的热点样本，用预取窗口提前准备下一 epoch 的可预测
// 局部性，并保证共享样本不会因为某个 worker 切换 epoch 而被误删。

export function planCache(config) {
  const {
    sampleSizes,
    workers,
    sharedSamples = [],
    cacheCapacityBytes,
    diskBandwidthBytesPerSecond,
    secondsPerStep = 1,
    prefetchWindow = 32,
    prefetchBatchBytes = Infinity,
  } = config ?? {};

  if (!sampleSizes || typeof sampleSizes !== 'object') {
    throw new TypeError('sampleSizes 必须是 { 样本ID: 字节数 } 的对象');
  }
  if (!Array.isArray(workers) || workers.length === 0) {
    throw new TypeError('workers 必须是非空数组');
  }
  if (!(cacheCapacityBytes > 0)) {
    throw new TypeError('cacheCapacityBytes 必须为正数');
  }
  if (!(diskBandwidthBytesPerSecond > 0)) {
    throw new TypeError('diskBandwidthBytesPerSecond 必须为正数');
  }

  const shared = new Set(sharedSamples);

  // 展开每个 worker 的跨 epoch 样本流。
  const workerStreams = workers.map((worker, workerIndex) => {
    if (!Array.isArray(worker.epochs) || worker.epochs.length === 0) {
      throw new TypeError(`worker ${worker.id ?? workerIndex} 缺少 epochs`);
    }
    const flat = [];
    const epochAt = [];
    worker.epochs.forEach((epochSamples, epochIndex) => {
      if (!Array.isArray(epochSamples) || epochSamples.length === 0) {
        throw new TypeError(
          `worker ${worker.id ?? workerIndex} 的 epoch ${epochIndex} 为空`,
        );
      }
      for (const sample of epochSamples) {
        if (!(sampleSizes[sample] > 0)) {
          throw new Error(`未知样本或非法大小: ${sample}`);
        }
        flat.push(sample);
        epochAt.push(epochIndex);
      }
    });
    return { id: worker.id ?? `worker-${workerIndex}`, flat, epochAt };
  });

  const maxLength = Math.max(...workerStreams.map((w) => w.flat.length));

  // 全局事件流：按 step 交错所有 worker 的访问。
  const stream = [];
  for (let step = 0; step < maxLength; step += 1) {
    for (const worker of workerStreams) {
      if (step < worker.flat.length) {
        stream.push({
          step,
          worker: worker.id,
          epoch: worker.epochAt[step],
          sample: worker.flat[step],
        });
      }
    }
  }

  // 每个样本在流中的使用位置，用于均摊 O(1) 的“下次使用”查询。
  const usePositions = new Map();
  stream.forEach((event, index) => {
    if (!usePositions.has(event.sample)) usePositions.set(event.sample, []);
    usePositions.get(event.sample).push(index);
  });
  const usePointers = new Map(
    [...usePositions.keys()].map((sample) => [sample, 0]),
  );
  function nextUseAfter(sample, streamIndex) {
    const positions = usePositions.get(sample);
    let pointer = usePointers.get(sample);
    while (pointer < positions.length && positions[pointer] <= streamIndex) {
      pointer += 1;
    }
    usePointers.set(sample, pointer);
    return pointer < positions.length ? positions[pointer] : Infinity;
  }

  const cache = new Map(); // sample -> sizeBytes
  let cacheBytes = 0;
  const evictions = [];
  const uncacheable = new Set();

  // 淘汰：下次使用最远者先出；共享样本只要还有任何 worker 的未来使用就受
  // 保护，仅在全部候选都受保护时退化为淘汰下次使用最远的受保护样本，
  // 避免缓存无法腾出空间。
  function makeRoom(sizeBytes, streamIndex) {
    if (sizeBytes > cacheCapacityBytes) return false;
    while (cacheBytes + sizeBytes > cacheCapacityBytes && cache.size > 0) {
      let victim = null;
      let victimNextUse = -1;
      let fallback = null;
      let fallbackNextUse = -1;
      for (const sample of cache.keys()) {
        const nextUse = nextUseAfter(sample, streamIndex);
        const isProtected = shared.has(sample) && nextUse !== Infinity;
        if (!isProtected && nextUse > victimNextUse) {
          victim = sample;
          victimNextUse = nextUse;
        }
        if (nextUse > fallbackNextUse) {
          fallback = sample;
          fallbackNextUse = nextUse;
        }
      }
      if (victim === null) victim = fallback;
      if (victim === null) break;
      evictions.push({
        step: stream[streamIndex].step,
        sample: victim,
        shared: shared.has(victim),
        bytesFreed: cache.get(victim),
      });
      cacheBytes -= cache.get(victim);
      cache.delete(victim);
    }
    return cacheBytes + sizeBytes <= cacheCapacityBytes;
  }

  function insert(sample, streamIndex) {
    if (cache.has(sample)) return true;
    const size = sampleSizes[sample];
    if (!makeRoom(size, streamIndex)) {
      uncacheable.add(sample);
      return false;
    }
    cache.set(sample, size);
    cacheBytes += size;
    return true;
  }

  const loadSeconds = (sample) =>
    sampleSizes[sample] / diskBandwidthBytesPerSecond;

  let now = 0;
  let diskFreeAt = 0;
  const arrivals = []; // { finish, sample }，finish 单调非降
  let arrivalPointer = 0;
  const scheduled = new Map(); // sample -> finish
  const prefetchBatches = [];
  const stallEvents = [];
  const epochStats = new Map();
  const cacheSnapshots = [];
  let hits = 0;
  let misses = 0;
  let totalStallSeconds = 0;

  function processArrivals(streamIndex) {
    while (
      arrivalPointer < arrivals.length &&
      arrivals[arrivalPointer].finish <= now
    ) {
      const arrival = arrivals[arrivalPointer];
      arrivalPointer += 1;
      scheduled.delete(arrival.sample);
      insert(arrival.sample, streamIndex);
    }
  }

  for (let index = 0; index < stream.length; index += 1) {
    const event = stream[index];
    now = Math.max(now, event.step * secondsPerStep);
    processArrivals(index);

    const epochKey = `${event.worker}#${event.epoch}`;
    if (!epochStats.has(epochKey)) {
      epochStats.set(epochKey, {
        worker: event.worker,
        epoch: event.epoch,
        hits: 0,
        misses: 0,
        stallSeconds: 0,
      });
      cacheSnapshots.push({
        step: event.step,
        worker: event.worker,
        epoch: event.epoch,
        samples: [...cache.keys()],
        usedBytes: cacheBytes,
      });
    }
    const stats = epochStats.get(epochKey);

    if (cache.has(event.sample)) {
      hits += 1;
      stats.hits += 1;
    } else if (scheduled.has(event.sample)) {
      // 预取在途但未完成：GPU 等待剩余传输时间。
      const finish = scheduled.get(event.sample);
      const stall = finish - now;
      misses += 1;
      stats.misses += 1;
      totalStallSeconds += stall;
      stats.stallSeconds += stall;
      stallEvents.push({
        step: event.step,
        worker: event.worker,
        epoch: event.epoch,
        sample: event.sample,
        seconds: stall,
        reason: 'prefetch-in-flight',
      });
      now = finish;
      processArrivals(index);
      if (!cache.has(event.sample)) insert(event.sample, index);
    } else {
      // 冷 miss：排队占用磁盘带宽加载。
      const start = Math.max(now, diskFreeAt);
      const finish = start + loadSeconds(event.sample);
      diskFreeAt = finish;
      const stall = finish - now;
      misses += 1;
      stats.misses += 1;
      if (stall > 0) {
        totalStallSeconds += stall;
        stats.stallSeconds += stall;
        stallEvents.push({
          step: event.step,
          worker: event.worker,
          epoch: event.epoch,
          sample: event.sample,
          seconds: stall,
          reason: 'demand-load',
        });
      }
      now = finish;
      insert(event.sample, index);
    }

    // 预取：在预取窗口内按访问顺序找出尚未缓存的未来样本，只要能在其
    // 使用时间之前完成传输就排入磁盘队列。跨 epoch 边界的样本同样会被
    // 提前准备（下一 epoch 的可预测局部性）。
    if (prefetchWindow > 0) {
      let batch = [];
      let batchBytes = 0;
      const flushBatch = () => {
        if (batch.length === 0) return;
        prefetchBatches.push({
          step: event.step,
          timeSeconds: now,
          samples: batch,
          bytes: batchBytes,
          completesAt: diskFreeAt,
        });
        batch = [];
        batchBytes = 0;
      };
      const seen = new Set();
      const lookaheadEnd = Math.min(stream.length, index + 1 + prefetchWindow);
      for (let j = index + 1; j < lookaheadEnd; j += 1) {
        const sample = stream[j].sample;
        if (seen.has(sample) || cache.has(sample) || scheduled.has(sample)) {
          continue;
        }
        seen.add(sample);
        if (sampleSizes[sample] > cacheCapacityBytes) continue;
        const start = Math.max(now, diskFreeAt);
        const finish = start + loadSeconds(sample);
        const useTime = stream[j].step * secondsPerStep;
        if (finish > useTime) continue;
        diskFreeAt = finish;
        scheduled.set(sample, finish);
        arrivals.push({ finish, sample });
        batch.push(sample);
        batchBytes += sampleSizes[sample];
        if (batchBytes >= prefetchBatchBytes) flushBatch();
      }
      flushBatch();
    }
  }

  return {
    totalAccesses: stream.length,
    hits,
    misses,
    hitRate: stream.length === 0 ? 0 : hits / stream.length,
    totalStallSeconds,
    stallEvents,
    evictions,
    prefetchBatches,
    epochs: [...epochStats.values()],
    cacheSnapshots,
    finalCache: {
      samples: [...cache.keys()],
      usedBytes: cacheBytes,
      capacityBytes: cacheCapacityBytes,
    },
    uncacheableSamples: [...uncacheable],
  };
}
