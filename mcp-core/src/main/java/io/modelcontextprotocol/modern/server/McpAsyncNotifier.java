/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.LoggingLevel;
import reactor.core.publisher.Mono;

/**
 * Emits request-scoped notifications from inside a {@link McpHandler.Streaming}.
 * {@link #progress} and {@link #log} are no-ops when the request declared no
 * {@code progressToken} / {@code logLevel}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpAsyncNotifier {

	/**
	 * Send a progress update. A no-op if the request carries no progress token, or if
	 * {@code progress} does not strictly increase over the previous call.
	 */
	Mono<Void> progress(double progress, Double total, String message);

	/**
	 * Send a log message. A no-op if the request declared no {@code logLevel}, or if
	 * {@code level} is less severe than the declared threshold.
	 */
	Mono<Void> log(LoggingLevel level, String logger, Object data);

	/** Send an arbitrary request-scoped notification, for use by extensions. */
	Mono<Void> notify(String method, Object params);

}
