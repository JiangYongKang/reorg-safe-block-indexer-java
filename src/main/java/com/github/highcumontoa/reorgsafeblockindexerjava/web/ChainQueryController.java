package com.github.highcumontoa.reorgsafeblockindexerjava.web;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.ChainState;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.SyncWatermark;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexerService;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.FailureType;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.SyncResult;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 查询与同步触发接口。
 * <ul>
 *   <li>GET /api/chain             当前规范链快照（含水位）</li>
 *   <li>GET /api/watermark         当前同步水位</li>
 *   <li>GET /api/blocks/{height}   按高度查询</li>
 *   <li>GET /api/blocks/hash/{h}   按哈希查询</li>
 *   <li>GET /api/blocks?from=&to=  高度区间查询</li>
 *   <li>POST /api/sync             触发一次同步</li>
 *   <li>POST /api/blocks           投递单个区块（重复投递幂等）</li>
 * </ul>
 * 查询接口只读取已发布快照，同步进行中也能稳定返回上一个一致视图。
 */
@RestController
@RequestMapping("/api")
public final class ChainQueryController {

    private final BlockIndexerService indexer;

    public ChainQueryController(BlockIndexerService indexer) {
        this.indexer = indexer;
    }

    @GetMapping("/chain")
    public ChainState chain() {
        return indexer.snapshot();
    }

    @GetMapping("/watermark")
    public SyncWatermark watermark() {
        return indexer.watermark();
    }

    @GetMapping("/blocks/{height}")
    public ResponseEntity<Block> byHeight(@PathVariable long height) {
        return indexer.blockAt(height)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/blocks/hash/{hash}")
    public ResponseEntity<Block> byHash(@PathVariable String hash) {
        return indexer.findByHash(hash)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/blocks")
    public List<Block> range(@RequestParam("from") long from, @RequestParam("to") long to) {
        return indexer.range(from, to);
    }

    @PostMapping("/sync")
    public ResponseEntity<Object> sync() {
        return toResponse(indexer.syncOnce());
    }

    @PostMapping("/blocks")
    public ResponseEntity<Object> ingest(@RequestBody Block block) {
        return toResponse(indexer.ingest(block));
    }

    private ResponseEntity<Object> toResponse(SyncResult result) {
        if (result.success()) {
            return ResponseEntity.ok(result);
        }
        HttpStatus status = switch (result.failureType()) {
            case NO_SOURCE -> HttpStatus.SERVICE_UNAVAILABLE;
            case BUSINESS -> HttpStatus.CONFLICT;
            case TIMEOUT, TRANSPORT -> HttpStatus.BAD_GATEWAY;
        };
        ApiError error = new ApiError(result.failureType().name(), result.failureMessage(),
                result.attemptedSources());
        return ResponseEntity.status(status).body(error);
    }
}
