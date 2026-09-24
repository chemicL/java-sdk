/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.server.McpRequestContext;

/**
 * The blocking counterpart of {@link McpAsyncPromptRepository}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpSyncPromptRepository {

	PromptsPage list(McpRequestContext ctx, String cursor);

	/**
	 * @return the handler for {@code name}, or {@code null} if it doesn't exist
	 */
	SyncPromptHandler resolve(McpRequestContext ctx, String name);

}
