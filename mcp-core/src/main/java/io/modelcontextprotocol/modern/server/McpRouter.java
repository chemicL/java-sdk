/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import reactor.core.publisher.Mono;

/**
 * Resolves a request's method to a handler. Routers have no maps: each implementation
 * (typically one per feature) matches its own method names directly, and composes with
 * others via {@link #and(McpRouter)}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpRouter {

	/**
	 * Resolve a handler for the request in {@code ctx}.
	 * @return a handler, or {@link Mono#empty()} if this router doesn't own the method
	 */
	Mono<McpHandler> route(McpRequestContext ctx);

	/**
	 * Try this router first, falling back to {@code other} if this one doesn't match.
	 */
	default McpRouter and(McpRouter other) {
		return ctx -> this.route(ctx).switchIfEmpty(Mono.defer(() -> other.route(ctx)));
	}

	static McpRouter empty() {
		return ctx -> Mono.empty();
	}

}
