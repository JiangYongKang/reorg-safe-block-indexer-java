package com.github.highcumontoa.reorgsafeblockindexerjava.service;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTimeoutException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTransportException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * 数据源路由器：按优先级在多个数据源之间选择并故障切换。
 * <p>规则：
 * <ul>
 *   <li>只选择能力满足需求（maxHeight 覆盖目标高度、批量声明支持）的源；</li>
 *   <li>按 priority 升序尝试；超时/传输类故障自动切换下一个源；</li>
 *   <li>业务错误也尝试下一个源，以便用健康源交叉验证；
 *       所有候选源均失败时按「业务 &gt; 传输 &gt; 超时」的主导类别给出可解释结论。</li>
 * </ul>
 */
public final class SourceRouter {

    /** 一次路由调用的结果：成功则携带数据源与数据，失败则携带全部尝试记录。 */
    public record Routed<T>(boolean success, BlockSource source, T value,
                            List<SourceFailure> failures, FailureType dominantFailureType) {

        static <T> Routed<T> ok(BlockSource source, T value) {
            return new Routed<>(true, source, value, List.of(), null);
        }

        static <T> Routed<T> fail(List<SourceFailure> failures, FailureType dominant) {
            return new Routed<>(false, null, null, List.copyOf(failures), dominant);
        }
    }

    private final List<BlockSource> sources;

    public SourceRouter(List<BlockSource> sources) {
        List<BlockSource> sorted = new ArrayList<>(sources == null ? List.of() : sources);
        sorted.sort(Comparator.comparingInt(s -> s.capabilities().priority()));
        this.sources = List.copyOf(sorted);
    }

    /** 全部已注册源（按优先级排序）。 */
    public List<BlockSource> sources() {
        return sources;
    }

    /** 拉取链头。 */
    public Routed<Optional<Block>> fetchHead() {
        return route(sources, BlockSource::head);
    }

    /** 拉取单个高度区块；maxHeight 不足的源被跳过。 */
    public Routed<Block> fetchBlock(long height) {
        List<BlockSource> eligible = filterMaxHeight(height);
        return route(eligible, s -> s.fetchBlock(height));
    }

    /** 批量拉取 [from,to]，仅选择声明支持批量且高度足够的源。 */
    public Routed<List<Block>> fetchBlocks(long fromHeight, long toHeight) {
        List<BlockSource> eligible = new ArrayList<>();
        for (BlockSource s : sources) {
            if (s.capabilities().batchFetchSupported() && s.capabilities().maxHeight() >= toHeight) {
                eligible.add(s);
            }
        }
        return route(eligible, s -> s.fetchBlocks(fromHeight, toHeight));
    }

    private List<BlockSource> filterMaxHeight(long required) {
        List<BlockSource> eligible = new ArrayList<>();
        for (BlockSource s : sources) {
            if (s.capabilities().maxHeight() >= required) {
                eligible.add(s);
            }
        }
        return eligible;
    }

    private <T> Routed<T> route(List<BlockSource> eligible, Function<BlockSource, T> call) {
        if (eligible.isEmpty()) {
            return Routed.fail(List.of(), FailureType.NO_SOURCE);
        }
        List<SourceFailure> failures = new ArrayList<>();
        for (BlockSource source : eligible) {
            String name = source.capabilities().name();
            try {
                return Routed.ok(source, call.apply(source));
            } catch (SourceBusinessException e) {
                failures.add(new SourceFailure(name, FailureType.BUSINESS, e.getMessage()));
            } catch (SourceTimeoutException e) {
                failures.add(new SourceFailure(name, FailureType.TIMEOUT, e.getMessage()));
            } catch (SourceTransportException e) {
                failures.add(new SourceFailure(name, FailureType.TRANSPORT, e.getMessage()));
            }
        }
        return Routed.fail(failures, dominant(failures));
    }

    /** 主导失败类别：同数时按 BUSINESS &gt; TRANSPORT &gt; TIMEOUT 判定。 */
    private FailureType dominant(List<SourceFailure> failures) {
        int business = 0;
        int transport = 0;
        int timeout = 0;
        for (SourceFailure f : failures) {
            switch (f.type()) {
                case BUSINESS -> business++;
                case TRANSPORT -> transport++;
                case TIMEOUT -> timeout++;
                default -> { }
            }
        }
        if (business >= transport && business >= timeout && business > 0) {
            return FailureType.BUSINESS;
        }
        if (transport >= timeout && transport > 0) {
            return FailureType.TRANSPORT;
        }
        if (timeout > 0) {
            return FailureType.TIMEOUT;
        }
        return FailureType.NO_SOURCE;
    }
}
