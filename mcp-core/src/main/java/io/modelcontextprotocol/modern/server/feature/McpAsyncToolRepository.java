/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import reactor.core.publisher.Mono;

/**
 * User-implemented catalogue of tools. There is no built-in map: {@link #resolve}
 * decides, per call, which handler answers a given tool name.
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
	 * Resolve the handler for {@code name}.
	 * @return the handler, or {@link Mono#empty()} if no such tool exists (answered as
	 * {@code -32602})
	 */
	Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx, String name);

}
