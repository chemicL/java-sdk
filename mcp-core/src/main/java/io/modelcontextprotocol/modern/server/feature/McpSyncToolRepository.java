/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpSchema.CallToolOutcome;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncResponse;

/**
 * The blocking counterpart of {@link McpAsyncToolRepository}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpSyncToolRepository {

	ToolsPage list(McpRequestContext ctx, String cursor);

	McpSyncResponse<CallToolOutcome> call(McpRequestContext ctx, CallToolRequest request);

}
