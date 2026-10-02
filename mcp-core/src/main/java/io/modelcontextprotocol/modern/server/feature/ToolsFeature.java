/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.Set;

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpError;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.ListToolsResult;
import io.modelcontextprotocol.modern.McpSchema.PaginatedRequest;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * The {@code tools/list} and {@code tools/call} feature: delegates entirely to a
 * user-supplied {@link McpAsyncToolRepository} (or its sync counterpart).
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
		this.repository = repository;
		this.jsonMapper = jsonMapper;
		this.defaultTtlMs = defaultTtlMs;
		this.defaultCacheScope = defaultCacheScope;
	}

	/** Uses the default JSON mapper and no caching. */
	public static ToolsFeature of(McpAsyncToolRepository repository) {
		return of(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	/** Uses the default JSON mapper and no caching. */
	public static ToolsFeature ofSync(McpSyncToolRepository repository) {
		return ofSync(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	public static ToolsFeature of(McpAsyncToolRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return new ToolsFeature(repository, jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	public static ToolsFeature ofSync(McpSyncToolRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return of(adapt(repository), jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	@Override
	public Set<String> methods() {
		return Set.of(McpSchema.METHOD_TOOLS_LIST, McpSchema.METHOD_TOOLS_CALL);
	}

	@Override
	public Mono<McpHandler> resolve(McpRequestContext ctx) {
		return McpSchema.METHOD_TOOLS_LIST.equals(ctx.method()) ? Mono.just(listHandler()) : callHandler(ctx);
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		builder.tools(false);
	}

	@Override
	public Set<String> inputRequiredMethods() {
		return Set.of(McpSchema.METHOD_TOOLS_CALL);
	}

	private McpHandler listHandler() {
		return (ctx, params) -> {
			PaginatedRequest request = params == null ? new PaginatedRequest(null, null)
					: this.jsonMapper.convertValue(params, PaginatedRequest.class);
			return this.repository.list(ctx, request.cursor()).map(this::toListResult);
		};
	}

	private Mono<McpHandler> callHandler(McpRequestContext ctx) {
		String name = ctx.primitiveName();
		if (name == null || name.isBlank()) {
			throw McpError.builder(ErrorCodes.INVALID_PARAMS).message("params.name is required").build();
		}
		return this.repository.resolve(ctx, name)
			.map(handler -> FeatureHandlers.toMcpHandler(handler, CallToolRequest.class, this.jsonMapper))
			.switchIfEmpty(
					Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS).message("Unknown tool: " + name).build()));
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
				return SyncAdapters.toAsync(ctx, () -> repository.list(ctx, cursor));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return SyncAdapters.toAsync(ctx, () -> repository.resolve(ctx, name)).map(SyncAdapters::toAsync);
			}
		};
	}

}
