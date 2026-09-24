/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.Set;

import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;

/**
 * A composable unit of server behaviour: the routes it owns, and the capabilities it
 * contributes to {@code server/discover}. Core primitives (tools, resources, prompts,
 * completions, subscriptions) are features; so is anything an extension adds.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpFeature {

	McpRouter router();

	/** Contribute this feature's advertised capabilities. Default: none. */
	default void capabilities(ServerCapabilities.Builder builder) {
	}

	/**
	 * The methods for which this feature's handlers may answer with an
	 * {@code InputRequiredResult}. Default: none.
	 */
	default Set<String> inputRequiredMethods() {
		return Set.of();
	}

}
