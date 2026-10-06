/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpException;
import io.modelcontextprotocol.modern.McpSchema.GetPromptOutcome;
import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpAsyncResponse;
import reactor.core.publisher.Mono;

/**
 * User-implemented catalogue of prompts.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpAsyncPromptRepository {

	/**
	 * List (a page of) the prompts. An unrecognized cursor is an
	 * {@link McpException#invalidParams(String) invalid-params error}.
	 */
	Mono<PromptsPage> list(McpRequestContext ctx, String cursor);

	/**
	 * Answer a {@code prompts/get}. An unknown prompt is an
	 * {@link McpException#invalidParams(String) invalid-params error}.
	 */
	Mono<McpAsyncResponse<GetPromptOutcome>> get(McpRequestContext ctx, GetPromptRequest request);

}
