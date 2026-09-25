package com.github.highcumontoa.reorgsafeblockindexerjava.web;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.ChainSnapshot;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 只读查询 + 同步触发的 HTTP 接口。
 *
 * <p>所有读接口基于不可变快照，任何时刻返回的都是某一次完整提交的链视图，
 * 不会出现回滚一半或写入一半的数据。
 */
@RestController
@RequestMapping("/api/index")
public class IndexController {

    private final BlockIndexer indexer;

    public IndexController(BlockIndexer indexer) {
        this.indexer = indexer;
    }

    /** 当前索引状态：水位 + 规范化链（高度/哈希/父哈希）。 */
    @GetMapping("/status")
    public Map<String, Object> status() {
        ChainSnapshot s = indexer.snapshot();
        return Map.of(
                "watermark", s.watermark(),
                "depth", s.blocks().size(),
                "blocks", s.blocks().stream()
                        .map(b -> Map.of("height", b.height(), "hash", b.hash(),
                                "parentHash", b.parentHash()))
                        .toList()
        );
    }

    /** 按高度查询区块。 */
    @GetMapping("/blocks/{height}")
    public ResponseEntity<Block> blockAtHeight(@PathVariable long height) {
        return indexer.snapshot()
                .blocks().stream()
                .filter(b -> b.height() == height)
                .findFirst()
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** 按哈希查询区块。 */
    @GetMapping("/blocks/by-hash/{hash}")
    public ResponseEntity<Block> blockByHash(@PathVariable String hash) {
        return indexer.snapshot()
                .blocks().stream()
                .filter(b -> b.hash().equals(hash))
                .findFirst()
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** 触发一次同步，返回可解释结果（状态、水位变化、回滚/重放、逐源尝试）。 */
    @PostMapping("/sync")
    public SyncResult sync() {
        return indexer.sync();
    }

    /** 直接投递一个区块（幂等；不合法时返回 REJECTED 且不写入）。 */
    @PostMapping("/blocks")
    public SyncResult submit(@RequestBody Block block) {
        return indexer.submitBlock(block);
    }
}
