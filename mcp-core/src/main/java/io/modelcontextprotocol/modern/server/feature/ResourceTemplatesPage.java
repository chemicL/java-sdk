/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;

import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.spec.McpSchema.ResourceTemplate;

/**
 * A page of {@code resources/templates/list}.
 *
 * @author Dariusz Jędrzejczyk
 */
public record ResourceTemplatesPage(List<ResourceTemplate> resourceTemplates, String nextCursor, Long ttlMs,
		CacheScope cacheScope) {

	public static ResourceTemplatesPage of(List<ResourceTemplate> resourceTemplates) {
		return new ResourceTemplatesPage(resourceTemplates, null, null, null);
	}

}
