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
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.McpSchema.GetPromptResult;
import io.modelcontextprotocol.modern.McpSchema.ListPromptsResult;
import io.modelcontextprotocol.modern.McpSchema.PaginatedRequest;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * The {@code prompts/list} and {@code prompts/get} feature.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class PromptsFeature implements McpFeature {

	private final McpAsyncPromptRepository repository;

	private final McpJsonMapper jsonMapper;

	private final long defaultTtlMs;

	private final CacheScope defaultCacheScope;

	private PromptsFeature(McpAsyncPromptRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		this.repository = repository;
		this.jsonMapper = jsonMapper;
		this.defaultTtlMs = defaultTtlMs;
		this.defaultCacheScope = defaultCacheScope;
	}

	/** Uses the default JSON mapper and no caching. */
	public static PromptsFeature of(McpAsyncPromptRepository repository) {
		return of(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	/** Uses the default JSON mapper and no caching. */
	public static PromptsFeature ofSync(McpSyncPromptRepository repository) {
		return ofSync(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	public static PromptsFeature of(McpAsyncPromptRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return new PromptsFeature(repository, jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	public static PromptsFeature ofSync(McpSyncPromptRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return of(adapt(repository), jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	@Override
	public Set<String> methods() {
		return Set.of(McpSchema.METHOD_PROMPTS_LIST, McpSchema.METHOD_PROMPTS_GET);
	}

	@Override
	public Mono<McpHandler> resolve(McpRequestContext ctx) {
		return McpSchema.METHOD_PROMPTS_LIST.equals(ctx.method()) ? Mono.just(listHandler()) : getHandler(ctx);
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		builder.prompts(false);
	}

	@Override
	public Set<String> inputRequiredMethods() {
		return Set.of(McpSchema.METHOD_PROMPTS_GET);
	}

	private McpHandler listHandler() {
		return (ctx, params) -> {
			PaginatedRequest request = params == null ? new PaginatedRequest(null, null)
					: this.jsonMapper.convertValue(params, PaginatedRequest.class);
			return this.repository.list(ctx, request.cursor()).map(this::toListResult);
		};
	}

	private Mono<McpHandler> getHandler(McpRequestContext ctx) {
		String name = ctx.primitiveName();
		if (name == null || name.isBlank()) {
			throw McpError.builder(ErrorCodes.INVALID_PARAMS).message("params.name is required").build();
		}
		return this.repository.resolve(ctx, name)
			.map(handler -> FeatureHandlers.toMcpHandler(handler, GetPromptRequest.class, this.jsonMapper))
			.switchIfEmpty(
					Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS).message("Unknown prompt: " + name).build()));
	}

	private Result toListResult(PromptsPage page) {
		return ListPromptsResult.builder(page.prompts())
			.nextCursor(page.nextCursor())
			.ttlMs(page.ttlMs() != null ? page.ttlMs() : this.defaultTtlMs)
			.cacheScope(page.cacheScope() != null ? page.cacheScope() : this.defaultCacheScope)
			.build();
	}

	private static McpAsyncPromptRepository adapt(McpSyncPromptRepository repository) {
		return new McpAsyncPromptRepository() {
			@Override
			public Mono<PromptsPage> list(McpRequestContext ctx, String cursor) {
				return SyncAdapters.toAsync(ctx, () -> repository.list(ctx, cursor));
			}

			@Override
			public Mono<AsyncFeatureHandler<GetPromptRequest, GetPromptResult>> resolve(McpRequestContext ctx,
					String name) {
				return SyncAdapters.toAsync(ctx, () -> repository.resolve(ctx, name)).map(SyncAdapters::toAsync);
			}
		};
	}

}
