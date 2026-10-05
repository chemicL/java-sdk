/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpException;
import io.modelcontextprotocol.modern.McpSchema.CallToolOutcome;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpAsyncResponse;
import reactor.core.publisher.Mono;

/**
 * User-implemented catalogue of tools.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpAsyncToolRepository {

	/**
	 * List (a page of) the tools this repository exposes. Must be deterministically
	 * ordered and must not vary by connection.
	 */
	Mono<ToolsPage> list(McpRequestContext ctx, String cursor);

	/**
	 * Answer a {@code tools/call}. An unknown tool is an
	 * {@link McpException#invalidParams(String) invalid-params error}.
	 */
	Mono<McpAsyncResponse<CallToolOutcome>> call(McpRequestContext ctx, CallToolRequest request);

}
