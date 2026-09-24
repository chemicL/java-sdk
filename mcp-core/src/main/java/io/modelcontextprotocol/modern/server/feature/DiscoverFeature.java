/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;

import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.DiscoverResult;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRouter;
import reactor.core.publisher.Mono;

/**
 * Always-present feature answering {@code server/discover}. {@code McpServer.Builder}
 * prepends this ahead of every other feature.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class DiscoverFeature implements McpFeature {

	private final List<String> supportedVersions;

	private final ServerCapabilities capabilities;

	private final String instructions;

	private final long ttlMs;

	private final CacheScope cacheScope;

	public DiscoverFeature(List<String> supportedVersions, ServerCapabilities capabilities, String instructions,
			long ttlMs, CacheScope cacheScope) {
		this.supportedVersions = supportedVersions;
		this.capabilities = capabilities;
		this.instructions = instructions;
		this.ttlMs = ttlMs;
		this.cacheScope = cacheScope;
	}

	@Override
	public McpRouter router() {
		return ctx -> {
			if (!McpSchema.METHOD_SERVER_DISCOVER.equals(ctx.method())) {
				return Mono.empty();
			}
			McpHandler handler = (c,
					params) -> Mono.just(DiscoverResult.builder(this.supportedVersions, this.capabilities)
						.instructions(this.instructions)
						.ttlMs(this.ttlMs)
						.cacheScope(this.cacheScope)
						.build());
			return Mono.just(handler);
		};
	}

}
