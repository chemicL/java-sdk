/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

/**
 * A change a {@link McpChangeFeed} may emit for {@code subscriptions/listen} to forward.
 * Not sealed: an extension may emit its own change types, which
 * {@link SubscriptionsFeature} simply won't recognize and won't forward.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface ServerChange {

	record ToolsListChanged() implements ServerChange {
	}

	record PromptsListChanged() implements ServerChange {
	}

	record ResourcesListChanged() implements ServerChange {
	}

	record ResourceUpdated(String uri) implements ServerChange {
	}

}
