/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;

import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.spec.McpSchema.Resource;

/**
 * A page of {@code resources/list}. See {@link ToolsPage} for the caching-hint
 * convention.
 *
 * @author Dariusz Jędrzejczyk
 */
public record ResourcesPage(List<Resource> resources, String nextCursor, Long ttlMs, CacheScope cacheScope) {

	public static ResourcesPage of(List<Resource> resources) {
		return new ResourcesPage(resources, null, null, null);
	}

}
