package com.github.highcumontoa.reorgsafeblockindexerjava.store;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.ChainState;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.SyncWatermark;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 进程内存索引存储。
 * <p>链视图以不可变 {@link ChainState} 快照形式原子发布（volatile 引用），
 * 读取方无锁、永不阻塞；同步方在同步锁内构造好完整新快照后一次性替换引用，
 * 保证查询永远读到「上一个」或「下一个」完整快照，看不到回滚中间态或部分写入。
 */
public final class InMemoryIndexStore {

    private volatile ChainState state = ChainState.empty();

    public InMemoryIndexStore() {
    }

    /** 读取当前已发布的链快照（永远非 null）。 */
    public ChainState snapshot() {
        return state;
    }

    /** 原子发布新快照。旧快照继续服务已开始的读取，直到自然结束。 */
    public void publish(ChainState newState) {
        if (newState == null) {
            throw new IllegalArgumentException("newState must not be null");
        }
        this.state = newState;
    }

    /** 当前同步水位。水位随已发布快照单调递增。 */
    public SyncWatermark watermark() {
        return state.watermark();
    }

    public Optional<Block> blockAt(long height) {
        return state.blockAt(height);
    }

    public Optional<Block> findByHash(String hash) {
        return state.findByHash(hash);
    }

    /** 高度闭区间查询，参数倒序或越界时返回可用部分（不含 null）。 */
    public List<Block> range(long fromHeight, long toHeight) {
        ChainState s = this.state;
        List<Block> out = new ArrayList<>();
        if (fromHeight > toHeight) {
            return out;
        }
        for (long h = Math.max(0, fromHeight); h <= toHeight; h++) {
            Optional<Block> b = s.blockAt(h);
            if (b.isEmpty()) {
                break;
            }
            out.add(b.get());
        }
        return List.copyOf(out);
    }
}
