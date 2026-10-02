/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.List;
import java.util.Set;

import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.DiscoverResult;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;

/**
 * Always-present feature answering {@code server/discover}.
 *
 * @author Dariusz Jędrzejczyk
 */
final class DiscoverFeature implements McpFeature {

	private final McpHandler handler;

	DiscoverFeature(List<String> supportedVersions, ServerCapabilities capabilities, String instructions, long ttlMs,
			CacheScope cacheScope) {
		DiscoverResult result = DiscoverResult.builder(supportedVersions, capabilities)
			.instructions(instructions)
			.ttlMs(ttlMs)
			.cacheScope(cacheScope)
			.build();
		this.handler = (ctx, params) -> Mono.just(result);
	}

	@Override
	public Set<String> methods() {
		return Set.of(McpSchema.METHOD_SERVER_DISCOVER);
	}

	@Override
	public Mono<McpHandler> resolve(McpRequestContext ctx) {
		return Mono.just(this.handler);
	}

}
