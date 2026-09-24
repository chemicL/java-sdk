/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;

/**
 * The blocking counterpart of {@link AsyncStreamingFunction}.
 *
 * @author Dariusz Jędrzejczyk
 */
@FunctionalInterface
public interface SyncStreamingFunction<REQ, RES> {

	RES apply(McpRequestContext ctx, REQ request, McpSyncNotifier notifier);

}
