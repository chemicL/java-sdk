/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;

import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.spec.McpSchema.Tool;

/**
 * A page of {@code tools/list}. {@code ttlMs}/{@code cacheScope} are optional; when
 * absent, {@code ToolsFeature} substitutes the server's cache defaults.
 *
 * @author Dariusz Jędrzejczyk
 */
public record ToolsPage(List<Tool> tools, String nextCursor, Long ttlMs, CacheScope cacheScope) {

	public static ToolsPage of(List<Tool> tools) {
		return new ToolsPage(tools, null, null, null);
	}

}
