/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.Set;

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.CallToolOutcome;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.ListToolsResult;
import io.modelcontextprotocol.modern.McpSchema.PaginatedRequest;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpAsyncResponse;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * The {@code tools/list} and {@code tools/call} feature.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class ToolsFeature implements McpFeature {

	private final McpAsyncToolRepository repository;

	private final McpJsonMapper jsonMapper;

	private final long defaultTtlMs;

	private final CacheScope defaultCacheScope;

	private ToolsFeature(McpAsyncToolRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.isTrue(defaultTtlMs >= 0, "defaultTtlMs must not be negative");
		Assert.notNull(defaultCacheScope, "defaultCacheScope must not be null");
		this.repository = repository;
		this.jsonMapper = jsonMapper;
		this.defaultTtlMs = defaultTtlMs;
		this.defaultCacheScope = defaultCacheScope;
	}

	/** Uses the default JSON mapper and no caching. */
	public static ToolsFeature ofAsync(McpAsyncToolRepository repository) {
		return ofAsync(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	/** Uses the default JSON mapper and no caching. */
	public static ToolsFeature ofSync(McpSyncToolRepository repository) {
		return ofSync(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	public static ToolsFeature ofAsync(McpAsyncToolRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return new ToolsFeature(repository, jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	public static ToolsFeature ofSync(McpSyncToolRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return ofAsync(adapt(repository), jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	@Override
	public Set<String> methods() {
		return Set.of(McpSchema.METHOD_TOOLS_LIST, McpSchema.METHOD_TOOLS_CALL);
	}

	@Override
	public Mono<? extends McpAsyncResponse<? extends Result>> handle(McpRequestContext ctx, Object params) {
		if (McpSchema.METHOD_TOOLS_LIST.equals(ctx.method())) {
			return Params.decode(this.jsonMapper, params, PaginatedRequest.class)
				.flatMap(request -> this.repository.list(ctx, request.cursor()))
				.map(page -> McpAsyncResponse.result(toListResult(page)));
		}
		return Params.decode(this.jsonMapper, params, CallToolRequest.class)
			.flatMap(request -> this.repository.call(ctx, request));
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		builder.tools(false);
	}

	@Override
	public Set<String> inputRequiredMethods() {
		return Set.of(McpSchema.METHOD_TOOLS_CALL);
	}

	private Result toListResult(ToolsPage page) {
		return ListToolsResult.builder(page.tools())
			.nextCursor(page.nextCursor())
			.ttlMs(page.ttlMs() != null ? page.ttlMs() : this.defaultTtlMs)
			.cacheScope(page.cacheScope() != null ? page.cacheScope() : this.defaultCacheScope)
			.build();
	}

	private static McpAsyncToolRepository adapt(McpSyncToolRepository repository) {
		return new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return SyncAdapters.call(ctx, () -> repository.list(ctx, cursor));
			}

			@Override
			public Mono<McpAsyncResponse<CallToolOutcome>> call(McpRequestContext ctx, CallToolRequest request) {
				return SyncAdapters.respond(ctx, () -> repository.call(ctx, request));
			}
		};
	}

}
