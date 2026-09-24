/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.Result;
import reactor.core.publisher.Mono;

/**
 * The generic handler a {@link McpRouter} resolves to. Application code never implements
 * this directly; it implements the typed per-primitive handler interfaces (e.g.
 * {@code AsyncToolHandler}) that features adapt to this shape.
 * <p>
 * {@link Streaming} is a distinct sub-interface, checked with {@code instanceof} at
 * dispatch time, rather than a flag - so the transport knows whether to answer with
 * {@code application/json} or {@code text/event-stream} as soon as the handler is
 * resolved, before it runs.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpHandler {

	/** Handle a unary request and complete with its result. */
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
