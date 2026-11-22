import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planCache } from '../src/planner.js';

const sizes = (ids, bytes = 10) =>
  Object.fromEntries(ids.map((id) => [id, bytes]));

test('固定 shuffle：Belady 淘汰保留热点，跨 epoch 命中可复现', () => {
  const plan = planCache({
    sampleSizes: sizes(['a', 'b', 'c', 'd']),
    workers: [
      {
        id: 'w0',
        epochs: [
          ['a', 'b', 'c', 'd'],
          ['a', 'b', 'c', 'd'],
          ['a', 'b', 'c', 'd'],
        ],
      },
    ],
    cacheCapacityBytes: 30,
    diskBandwidthBytesPerSecond: 1000,
    prefetchWindow: 0,
  });

  assert.equal(plan.totalAccesses, 12);
  assert.equal(plan.hits, 6);
  assert.equal(plan.misses, 6);
  assert.equal(plan.evictions.length, 3);
  // 容量 30 容纳 3 个样本，最终缓存保留最近保留的集合。
  assert.deepEqual([...plan.finalCache.samples].sort(), ['b', 'c', 'd']);
  assert.equal(plan.finalCache.usedBytes, 30);
  // 每个 epoch 都有统计与缓存快照。
  assert.equal(plan.epochs.length, 3);
  assert.equal(plan.cacheSnapshots.length, 3);
});

test('固定 shuffle + 预取：下一 epoch 的局部性被提前准备', () => {
  const plan = planCache({
    sampleSizes: sizes(['a', 'b', 'c', 'd']),
    workers: [
      {
        id: 'w0',
        epochs: [
          ['a', 'b'],
          ['c', 'd'],
        ],
      },
    ],
    cacheCapacityBytes: 40,
    diskBandwidthBytesPerSecond: 10,
    secondsPerStep: 2,
    prefetchWindow: 4,
  });

  // 首个样本冷 miss 产生 1s 停顿，其余全部被预取覆盖。
  assert.equal(plan.misses, 1);
  assert.equal(plan.hits, 3);
  assert.equal(plan.totalStallSeconds, 1);
  // 预取批次跨越 epoch 边界：下一 epoch 的 c、d 被提前加载。
  assert.equal(plan.prefetchBatches.length, 1);
  assert.deepEqual(plan.prefetchBatches[0].samples, ['b', 'c', 'd']);
  assert.equal(plan.prefetchBatches[0].bytes, 30);
});

test('动态采样：变长 epoch、有放回的重复样本、worker 长度不齐', () => {
  const plan = planCache({
    sampleSizes: sizes(['a', 'b', 'c', 'd']),
    workers: [
      { id: 'w0', epochs: [['a', 'b'], ['b', 'c', 'a'], ['d']] },
      { id: 'w1', epochs: [['c'], ['a', 'd', 'b', 'c']] },
    ],
    cacheCapacityBytes: 1_000_000,
    diskBandwidthBytesPerSecond: 1000,
    prefetchWindow: 0,
  });

  assert.equal(plan.totalAccesses, 11);
  assert.equal(plan.hits + plan.misses, 11);
  assert.ok(plan.hits > 0, '重复样本应命中');
  assert.equal(plan.evictions.length, 0, '容量充足不应淘汰');
  assert.equal(plan.epochs.length, 5);
  assert.deepEqual(
    [...plan.finalCache.samples].sort(),
    ['a', 'b', 'c', 'd'],
  );
});

test('共享样本：不会因某个 worker 切换 epoch 被删除', () => {
  const trace = {
    sampleSizes: sizes(['x', 'y', 'z', 'u', 'v', 'w']),
    sharedSamples: ['x'],
    workers: [
      { id: 'w0', epochs: [['x', 'y'], ['z', 'y']] },
      { id: 'w1', epochs: [['u'], ['v'], ['w'], ['x']] },
    ],
    cacheCapacityBytes: 20,
    diskBandwidthBytesPerSecond: 1000,
    prefetchWindow: 0,
  };

  const plan = planCache(trace);
  // x 在 w0 的 epoch 0 之后就不再被 w0 使用，但 w1 在最后仍需要它：
  // 纯 Belady 会在 step 1 淘汰下次使用最远的 x，保护规则使其保留。
  assert.equal(
    plan.evictions.filter((e) => e.sample === 'x').length,
    0,
    '共享样本 x 仍有未来使用时不应被淘汰',
  );
  assert.equal(plan.totalAccesses, 8);
  // w1 的 x 命中缓存。
  const w1Epoch3 = plan.epochs.find(
    (e) => e.worker === 'w1' && e.epoch === 3,
  );
  assert.equal(w1Epoch3.hits, 1);
  assert.equal(w1Epoch3.misses, 0);

  // 对照组：不标记共享时，x 会在 step 1 被淘汰，w1 的 x 变为 miss。
  const baseline = planCache({ ...trace, sharedSamples: [] });
  assert.ok(baseline.evictions.some((e) => e.sample === 'x'));
  const baselineW1Epoch3 = baseline.epochs.find(
    (e) => e.worker === 'w1' && e.epoch === 3,
  );
  assert.equal(baselineW1Epoch3.misses, 1);
});

test('容量不足：停顿与淘汰被如实报告，超大样本标记为不可缓存', () => {
  const plan = planCache({
    sampleSizes: { ...sizes(['a', 'b', 'c', 'd', 'e', 'f'], 100), big: 200 },
    workers: [
      {
        id: 'w0',
        epochs: [
          ['a', 'b', 'c', 'd', 'e', 'f', 'big'],
          ['a', 'b', 'c', 'd', 'e', 'f', 'big'],
        ],
      },
    ],
    cacheCapacityBytes: 150,
    diskBandwidthBytesPerSecond: 100,
    secondsPerStep: 1,
    prefetchWindow: 0,
  });

  // 容量只能容纳 1 个常规样本，工作集远大于缓存：全部 miss。
  assert.equal(plan.misses, 14);
  assert.equal(plan.hits, 0);
  assert.ok(plan.evictions.length > 0);
  // 每次常规 miss 停顿 1s（100B / 100Bps），big 停顿 2s。
  assert.equal(plan.totalStallSeconds, 12 * 1 + 2 * 2);
  assert.deepEqual(plan.uncacheableSamples, ['big']);
  assert.ok(
    plan.stallEvents.every((e) => e.seconds > 0),
    '每次 miss 都应记录停顿',
  );
});

test('非法输入直接抛错', () => {
  assert.throws(() => planCache({}), TypeError);
  assert.throws(
    () =>
      planCache({
        sampleSizes: { a: 10 },
        workers: [{ id: 'w0', epochs: [['unknown']] }],
        cacheCapacityBytes: 100,
        diskBandwidthBytesPerSecond: 10,
      }),
    /未知样本/,
  );
});
