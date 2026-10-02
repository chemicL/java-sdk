/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;

/**
 * The blocking counterpart of {@link McpAsyncResourceRepository}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpSyncResourceRepository {

	ResourcesPage list(McpRequestContext ctx, String cursor);

	default ResourceTemplatesPage listTemplates(McpRequestContext ctx, String cursor) {
		return ResourceTemplatesPage.of(java.util.List.of());
	}

	/**
	 * @return the handler for {@code uri}, or {@code null} if it doesn't exist
	 */
	SyncFeatureHandler<ReadResourceRequest, ReadResourceResult> resolve(McpRequestContext ctx, String uri);

}
