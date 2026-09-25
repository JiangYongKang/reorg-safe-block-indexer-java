package com.github.highcumontoa.reorgsafeblockindexerjava.source;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 本地进程内模拟区块源。
 * <p>链以区块序列表示，可通过 {@link #setChain(List)} 切换链视图以模拟分叉/链重组；
 * 可注入 {@link SourceTimeoutException}、{@link SourceTransportException}、
 * {@link SourceBusinessException} 三类故障，用于故障切换测试。
 * <p>超时为即时抛出（不真实等待），全部行为发生在本地进程内，无网络、无凭据。
 */
public final class InMemoryBlockSource implements BlockSource {

    private enum Kind { TIMEOUT, TRANSPORT, BUSINESS }

    private final SourceCapabilities capabilities;
    private final Object lock = new Object();
    private volatile List<Block> chain = List.of();
    private final Map<Long, Kind> oneShotFailures = new HashMap<>();
    private Kind headOneShotFailure;

    public InMemoryBlockSource(SourceCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public SourceCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public Optional<Block> head() {
        Kind kind;
        synchronized (lock) {
            kind = headOneShotFailure;
            headOneShotFailure = null;
        }
        if (kind != null) {
            throw raise(kind, -1);
        }
        List<Block> snapshot = chain;
        if (snapshot.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(snapshot.get(snapshot.size() - 1));
    }

    @Override
    public Block fetchBlock(long height) {
        Kind kind;
        synchronized (lock) {
            kind = oneShotFailures.remove(height);
        }
        if (kind != null) {
            throw raise(kind, height);
        }
        List<Block> snapshot = chain;
        if (height < 0 || height >= snapshot.size()) {
            throw new SourceBusinessException(capabilities.name(),
                    "height " + height + " is not available on this source");
        }
        Block b = snapshot.get((int) height);
        if (b.height() != height) {
            throw new SourceBusinessException(capabilities.name(),
                    "non-contiguous chain at requested height " + height);
        }
        return b;
    }

    @Override
    public List<Block> fetchBlocks(long fromHeight, long toHeight) {
        if (!capabilities.batchFetchSupported()) {
            throw new SourceBusinessException(capabilities.name(),
                    "batch fetch is not supported by this source");
        }
        if (fromHeight < 0 || toHeight < fromHeight) {
            throw new SourceBusinessException(capabilities.name(),
                    "invalid range [" + fromHeight + ", " + toHeight + "]");
        }
        List<Block> out = new ArrayList<>();
        for (long h = fromHeight; h <= toHeight; h++) {
            out.add(fetchBlock(h));
        }
        return List.copyOf(out);
    }

    private RuntimeException raise(Kind kind, long height) {
        String where = height < 0 ? "head" : ("height " + height);
        String name = capabilities.name();
        return switch (kind) {
            case TIMEOUT -> new SourceTimeoutException(name,
                    "simulated timeout from " + name + " at " + where, null);
            case TRANSPORT -> new SourceTransportException(name,
                    "simulated transport failure from " + name + " at " + where, null);
            case BUSINESS -> new SourceBusinessException(name,
                    "simulated business error from " + name + " at " + where);
        };
    }

    // ---- 测试辅助 API（仅本地模拟源使用） ----

    /** 以一条新链替换当前链视图（模拟链重组后数据源看到的新链）。 */
    public void setChain(List<Block> newChain) {
        List<Block> sorted = new ArrayList<>(newChain == null ? List.of() : newChain);
        sorted.sort((a, b) -> Long.compare(a.height(), b.height()));
        synchronized (lock) {
            this.chain = List.copyOf(sorted);
        }
    }

    /** 让指定高度的下一次读取触发超时。 */
    public void failOnceTimeout(long height) {
        synchronized (lock) {
            oneShotFailures.put(height, Kind.TIMEOUT);
        }
    }

    /** 让指定高度的下一次读取触发传输故障。 */
    public void failOnceTransport(long height) {
        synchronized (lock) {
            oneShotFailures.put(height, Kind.TRANSPORT);
        }
    }

    /** 让指定高度的下一次读取触发业务错误。 */
    public void failOnceBusiness(long height) {
        synchronized (lock) {
            oneShotFailures.put(height, Kind.BUSINESS);
        }
    }

    /** 让下一次链头读取触发超时。 */
    public void failHeadOnceTimeout() {
        synchronized (lock) {
            headOneShotFailure = Kind.TIMEOUT;
        }
    }

    /** 让下一次链头读取触发传输故障。 */
    public void failHeadOnceTransport() {
        synchronized (lock) {
            headOneShotFailure = Kind.TRANSPORT;
        }
    }

    /** 清空所有注入故障。 */
    public void clearFailures() {
        synchronized (lock) {
            oneShotFailures.clear();
            headOneShotFailure = null;
        }
    }
}
