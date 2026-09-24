/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRouter;
import io.modelcontextprotocol.spec.McpSchema.CompleteRequest;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * The {@code completion/complete} feature.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class CompletionsFeature implements McpFeature {

	private final McpAsyncCompletionRepository repository;

	private final McpJsonMapper jsonMapper;

	private CompletionsFeature(McpAsyncCompletionRepository repository, McpJsonMapper jsonMapper) {
		this.repository = repository;
		this.jsonMapper = jsonMapper;
	}

	public static CompletionsFeature of(McpAsyncCompletionRepository repository, McpJsonMapper jsonMapper) {
		Assert.notNull(repository, "repository must not be null");
		return new CompletionsFeature(repository, jsonMapper);
	}

	public static CompletionsFeature ofSync(McpSyncCompletionRepository repository, McpJsonMapper jsonMapper) {
		Assert.notNull(repository, "repository must not be null");
		return of((ctx, request) -> SyncAdapters.unary(() -> repository.complete(ctx, request)), jsonMapper);
	}

	@Override
	public McpRouter router() {
		return ctx -> {
			if (!McpSchema.METHOD_COMPLETION_COMPLETE.equals(ctx.method())) {
				return Mono.empty();
			}
			McpHandler handler = (c, params) -> {
				CompleteRequest request = this.jsonMapper.convertValue(params, CompleteRequest.class);
				return this.repository.complete(c, request).map(result -> result);
			};
			return Mono.just(handler);
		};
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		builder.completions();
	}

}
