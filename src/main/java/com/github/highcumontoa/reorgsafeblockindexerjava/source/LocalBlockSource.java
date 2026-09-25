package com.github.highcumontoa.reorgsafeblockindexerjava.source;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 本地进程内的模拟区块源。
 *
 * <p>持有一条可变的区块链（按高度排列的列表 + 哈希索引），可被测试代码：
 * <ul>
 *   <li>{@link #replaceChain} 整体替换为另一条分叉，模拟链重组；</li>
 *   <li>{@link #setFailure} 注入 TIMEOUT / TRANSPORT / BUSINESS 故障；</li>
 *   <li>{@link #setGate} 设置门闩，使数据请求阻塞，直到放行（用于“同步中可读”测试）；</li>
 *   <li>{@link #setLatencyMillis} 注入固定延迟，用于模拟超时。</li>
 * </ul>
 * 所有状态都在 JVM 进程内，无网络、无凭据。
 */
public class LocalBlockSource implements BlockSource {

    private final String name;
    private final Object lock = new Object();
    private List<Block> chain = new ArrayList<>();
    private Map<String, Block> byHash = new HashMap<>();

    private volatile BlockSourceException failure;
    private volatile java.util.concurrent.CountDownLatch gate;
    private volatile long latencyMillis;
    private volatile Runnable onAwaitGate;

    public LocalBlockSource(String name) {
        this.name = name;
    }

    /** 用一条新链整体替换当前链（用于模拟链源侧发生重组）。 */
    public void replaceChain(List<Block> newChain) {
        synchronized (lock) {
            this.chain = new ArrayList<>(newChain);
            Map<String, Block> m = new HashMap<>();
            for (Block b : this.chain) {
                m.put(b.hash(), b);
            }
            this.byHash = m;
        }
    }

    /** 注入故障；传 null 清除故障。 */
    public void setFailure(BlockSourceException failure) {
        this.failure = failure;
    }

    /** 设置请求门闩：数据请求会在门闩上等待（用于制造进行中的同步）。 */
    public void setGate(java.util.concurrent.CountDownLatch gate) {
        this.gate = gate;
    }

    /** 设置每次数据请求的固定延迟（毫秒）。 */
    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    /** 设置回调：请求在门闩上开始等待前触发（测试用于确认同步已在途）。 */
    public void setOnAwaitGate(Runnable onAwaitGate) {
        this.onAwaitGate = onAwaitGate;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long headHeight() throws BlockSourceException {
        awaitGate();
        applyLatency();
        maybeFail();
        synchronized (lock) {
            return chain.isEmpty() ? -1L : chain.get(chain.size() - 1).height();
        }
    }

    @Override
    public Optional<Block> blockAtHeight(long height) throws BlockSourceException {
        awaitGate();
        applyLatency();
        maybeFail();
        synchronized (lock) {
            if (height < 0 || height >= chain.size()) {
                return Optional.empty();
            }
            Block b = chain.get((int) height);
            if (b.height() != height) {
                throw new SourceBusinessException(
                        name + ": 高度 " + height + " 处的区块高度字段不一致");
            }
            return Optional.of(b);
        }
    }

    @Override
    public Optional<Block> blockByHash(String hash) throws BlockSourceException {
        awaitGate();
        applyLatency();
        maybeFail();
        synchronized (lock) {
            return Optional.ofNullable(byHash.get(hash));
        }
    }

    private void awaitGate() throws SourceTransportException {
        java.util.concurrent.CountDownLatch g = gate;
        if (g != null) {
            Runnable cb = onAwaitGate;
            if (cb != null) {
                cb.run();
            }
            try {
                g.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceTransportException(name + ": 等待数据时被中断", e);
            }
        }
    }

    private void applyLatency() throws SourceTimeoutException {
        long ms = latencyMillis;
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceTimeoutException(name + ": 延迟等待被中断", e);
            }
        }
    }

    private void maybeFail() throws BlockSourceException {
        BlockSourceException f = failure;
        if (f != null) {
            throw f;
        }
    }
}
