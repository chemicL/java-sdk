/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;

import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import reactor.core.publisher.Mono;

/**
 * User-implemented catalogue of resources and resource templates.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpAsyncResourceRepository {

	Mono<ResourcesPage> list(McpRequestContext ctx, String cursor);

	default Mono<ResourceTemplatesPage> listTemplates(McpRequestContext ctx, String cursor) {
		return Mono.just(ResourceTemplatesPage.of(List.of()));
	}

	/**
	 * @return the handler for {@code uri}, or {@link Mono#empty()} if it doesn't exist
	 * (answered as {@code -32602})
	 */
	Mono<AsyncFeatureHandler<ReadResourceRequest, ReadResourceResult>> resolve(McpRequestContext ctx, String uri);

}
