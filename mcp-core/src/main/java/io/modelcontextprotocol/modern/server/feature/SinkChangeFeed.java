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
 * {@link #emit} whenever something changes. A change emitted while no listen stream is
 * active is dropped.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class SinkChangeFeed implements McpChangeFeed {

	private static final Logger logger = LoggerFactory.getLogger(SinkChangeFeed.class);

	// directBestEffort neither buffers changes nobody listens to nor terminates when the
	// last listener leaves, unlike onBackpressureBuffer's warm-up buffer and autoCancel.
	private final Sinks.Many<ServerChange> sink = Sinks.many().multicast().directBestEffort();

	SinkChangeFeed() {
	}

	public void emit(ServerChange change) {
		// Multicast sinks reject concurrent producers, so a contended emit retries until
		// the other one is done; that one only hands its change to each listener's own
		// buffer. Not emitNext: it answers FAIL_OVERFLOW by erroring the sink, which
		// would end every listen stream for good.
		Sinks.EmitResult result;
		while ((result = this.sink.tryEmitNext(change)) == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
			Thread.onSpinWait();
		}
		if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
			logger.warn("Failed to emit change {}: {}", change, result);
		}
	}

	@Override
	public Flux<ServerChange> changes() {
		return this.sink.asFlux();
	}

}
