/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.Set;

import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;

/**
 * A composable unit of server behaviour: the methods it serves, and the capabilities it
 * contributes to {@code server/discover}. Core primitives (tools, resources, prompts,
 * completions) are features; so is anything an extension adds.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpFeature {

	/**
	 * The JSON-RPC methods this feature serves. No two features may serve the same one.
	 */
	Set<String> methods();

	/**
	 * Resolve the handler for a request whose method is one of {@link #methods()}.
	 * @return the handler, or {@link Mono#empty()} to answer {@code -32601}
	 */
	Mono<McpHandler> resolve(McpRequestContext ctx);

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
