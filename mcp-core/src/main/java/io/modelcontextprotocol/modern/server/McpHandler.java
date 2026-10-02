/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.Result;
import reactor.core.publisher.Mono;

/**
 * The generic handler a {@link McpFeature} resolves to. It is either single (one
 * response) or {@link Streaming} (may push notifications before its response).
 * Application code implements the typed feature handlers instead.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpHandler {

	/** Handle a request that is answered with a single response. */
	Mono<Result> handle(McpRequestContext ctx, Object params);

	/**
	 * A handler that may push request-scoped notifications (progress, log messages) while
	 * producing its result.
	 */
	interface Streaming extends McpHandler {

		Mono<Result> handle(McpRequestContext ctx, Object params, McpAsyncNotifier notifier);

		@Override
		default Mono<Result> handle(McpRequestContext ctx, Object params) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

}
