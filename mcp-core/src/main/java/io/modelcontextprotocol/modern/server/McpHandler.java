/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.Result;
import reactor.core.publisher.Mono;

/**
 * The generic handler a {@link McpFeature} resolves to. Application code never implements
 * this directly; it implements the typed {@code AsyncFeatureHandler} or
 * {@code SyncFeatureHandler} that features adapt to this shape.
 * <p>
 * A handler is either single (answered with one response) or {@link Streaming} (may push
 * notifications before its response). This is independent of sync vs async, which is only
 * the programming paradigm the application code is written in.
 * <p>
 * {@link Streaming} is a distinct sub-interface, checked with {@code instanceof} at
 * dispatch time, rather than a flag - so the transport knows whether to answer with
 * {@code application/json} or {@code text/event-stream} as soon as the handler is
 * resolved, before it runs.
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
