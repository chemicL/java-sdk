/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.McpSchema.GetPromptResult;
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
	SyncFeatureHandler<GetPromptRequest, GetPromptResult> resolve(McpRequestContext ctx, String name);

}
