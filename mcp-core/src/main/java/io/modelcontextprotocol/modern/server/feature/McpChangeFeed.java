/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import reactor.core.publisher.Flux;

/**
 * A hot source of {@link ServerChange}s for {@code subscriptions/listen} to forward. Each
 * subscriber (one per active listen stream) sees changes from the moment it subscribes
 * onward.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpChangeFeed {

	Flux<ServerChange> changes();

	/** Whether this feed can report {@code resources/updated} for a given uri. */
	default boolean supportsResourceUpdates() {
		return true;
	}

	/**
	 * A non-reactive entry point: {@code emit(...)} pushes a change to every active
	 * listen stream.
	 */
	static SinkChangeFeed sink() {
		return new SinkChangeFeed();
	}

}
