/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.McpSchema.GetPromptResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import reactor.core.publisher.Mono;

/**
 * User-implemented catalogue of prompts.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpAsyncPromptRepository {

	Mono<PromptsPage> list(McpRequestContext ctx, String cursor);

	/**
	 * @return the handler for {@code name}, or {@link Mono#empty()} if it doesn't exist
	 * (answered as {@code -32602})
	 */
	Mono<AsyncFeatureHandler<GetPromptRequest, GetPromptResult>> resolve(McpRequestContext ctx, String name);

}
