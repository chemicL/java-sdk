/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.LoggingLevel;

/**
 * The blocking counterpart of {@link McpAsyncNotifier}, for sync handlers. No Reactor
 * types appear in this interface.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpSyncNotifier {

	/** See {@link McpAsyncNotifier#progress}. */
	void progress(double progress, Double total, String message);

	/** See {@link McpAsyncNotifier#log}. */
	void log(LoggingLevel level, String logger, Object data);

	/** See {@link McpAsyncNotifier#notify}. */
	void notify(String method, Object params);

}
