/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * A {@link McpChangeFeed} for callers with no reactive source of their own: call
 * {@link #emit} whenever something changes.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class SinkChangeFeed implements McpChangeFeed {

	private static final Logger logger = LoggerFactory.getLogger(SinkChangeFeed.class);

	private final Sinks.Many<ServerChange> sink = Sinks.many().multicast().onBackpressureBuffer();

	SinkChangeFeed() {
	}

	public void emit(ServerChange change) {
		Sinks.EmitResult result = this.sink.tryEmitNext(change);
		if (result.isFailure()) {
			logger.warn("Failed to emit change {}: {}", change, result);
		}
	}

	@Override
	public Flux<ServerChange> changes() {
		return this.sink.asFlux();
	}

}
