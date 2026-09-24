/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.server.McpRequestContext;

/**
 * The blocking counterpart of {@link McpAsyncToolRepository}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpSyncToolRepository {

	ToolsPage list(McpRequestContext ctx, String cursor);

	/**
	 * @return the handler, or {@code null} if no such tool exists
	 */
	SyncToolHandler resolve(McpRequestContext ctx, String name);

}
