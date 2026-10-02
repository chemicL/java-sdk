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
import io.modelcontextprotocol.modern.McpSchema.ListResourceTemplatesResult;
import io.modelcontextprotocol.modern.McpSchema.ListResourcesResult;
import io.modelcontextprotocol.modern.McpSchema.PaginatedRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * The {@code resources/list}, {@code resources/templates/list} and {@code resources/read}
 * feature.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class ResourcesFeature implements McpFeature {

	private final McpAsyncResourceRepository repository;

	private final McpJsonMapper jsonMapper;

	private final long defaultTtlMs;

	private final CacheScope defaultCacheScope;

	private ResourcesFeature(McpAsyncResourceRepository repository, McpJsonMapper jsonMapper, long defaultTtlMs,
			CacheScope defaultCacheScope) {
		this.repository = repository;
		this.jsonMapper = jsonMapper;
		this.defaultTtlMs = defaultTtlMs;
		this.defaultCacheScope = defaultCacheScope;
	}

	/** Uses the default JSON mapper and no caching. */
	public static ResourcesFeature of(McpAsyncResourceRepository repository) {
		return of(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	/** Uses the default JSON mapper and no caching. */
	public static ResourcesFeature ofSync(McpSyncResourceRepository repository) {
		return ofSync(repository, McpJsonDefaults.getMapper(), 0L, CacheScope.PRIVATE);
	}

	public static ResourcesFeature of(McpAsyncResourceRepository repository, McpJsonMapper jsonMapper,
			long defaultTtlMs, CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return new ResourcesFeature(repository, jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	public static ResourcesFeature ofSync(McpSyncResourceRepository repository, McpJsonMapper jsonMapper,
			long defaultTtlMs, CacheScope defaultCacheScope) {
		Assert.notNull(repository, "repository must not be null");
		return of(adapt(repository), jsonMapper, defaultTtlMs, defaultCacheScope);
	}

	@Override
	public Set<String> methods() {
		return Set.of(McpSchema.METHOD_RESOURCES_LIST, McpSchema.METHOD_RESOURCES_TEMPLATES_LIST,
				McpSchema.METHOD_RESOURCES_READ);
	}

	@Override
	public Mono<McpHandler> resolve(McpRequestContext ctx) {
		return switch (ctx.method()) {
			case McpSchema.METHOD_RESOURCES_LIST -> Mono.just(listHandler());
			case McpSchema.METHOD_RESOURCES_TEMPLATES_LIST -> Mono.just(listTemplatesHandler());
			default -> readHandler(ctx);
		};
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		builder.resources(false, false);
	}

	@Override
	public Set<String> inputRequiredMethods() {
		return Set.of(McpSchema.METHOD_RESOURCES_READ);
	}

	private McpHandler listHandler() {
		return (ctx, params) -> {
			PaginatedRequest request = params == null ? new PaginatedRequest(null, null)
					: this.jsonMapper.convertValue(params, PaginatedRequest.class);
			return this.repository.list(ctx, request.cursor()).map(this::toListResult);
		};
	}

	private McpHandler listTemplatesHandler() {
		return (ctx, params) -> {
			PaginatedRequest request = params == null ? new PaginatedRequest(null, null)
					: this.jsonMapper.convertValue(params, PaginatedRequest.class);
			return this.repository.listTemplates(ctx, request.cursor()).map(this::toListTemplatesResult);
		};
	}

	private Mono<McpHandler> readHandler(McpRequestContext ctx) {
		String uri = ctx.primitiveName();
		if (uri == null || uri.isBlank()) {
			throw McpError.builder(ErrorCodes.INVALID_PARAMS).message("params.uri is required").build();
		}
		return this.repository.resolve(ctx, uri)
			.map(handler -> FeatureHandlers.toMcpHandler(handler, ReadResourceRequest.class, this.jsonMapper))
			.switchIfEmpty(Mono
				.error(McpError.builder(ErrorCodes.INVALID_PARAMS).message("Unknown resource: " + uri).build()));
	}

	private Result toListResult(ResourcesPage page) {
		return ListResourcesResult.builder(page.resources())
			.nextCursor(page.nextCursor())
			.ttlMs(page.ttlMs() != null ? page.ttlMs() : this.defaultTtlMs)
			.cacheScope(page.cacheScope() != null ? page.cacheScope() : this.defaultCacheScope)
			.build();
	}

	private Result toListTemplatesResult(ResourceTemplatesPage page) {
		return ListResourceTemplatesResult.builder(page.resourceTemplates())
			.nextCursor(page.nextCursor())
			.ttlMs(page.ttlMs() != null ? page.ttlMs() : this.defaultTtlMs)
			.cacheScope(page.cacheScope() != null ? page.cacheScope() : this.defaultCacheScope)
			.build();
	}

	private static McpAsyncResourceRepository adapt(McpSyncResourceRepository repository) {
		return new McpAsyncResourceRepository() {
			@Override
			public Mono<ResourcesPage> list(McpRequestContext ctx, String cursor) {
				return SyncAdapters.toAsync(ctx, () -> repository.list(ctx, cursor));
			}

			@Override
			public Mono<ResourceTemplatesPage> listTemplates(McpRequestContext ctx, String cursor) {
				return SyncAdapters.toAsync(ctx, () -> repository.listTemplates(ctx, cursor));
			}

			@Override
			public Mono<AsyncFeatureHandler<ReadResourceRequest, ReadResourceResult>> resolve(McpRequestContext ctx,
					String uri) {
				return SyncAdapters.toAsync(ctx, () -> repository.resolve(ctx, uri)).map(SyncAdapters::toAsync);
			}
		};
	}

}
