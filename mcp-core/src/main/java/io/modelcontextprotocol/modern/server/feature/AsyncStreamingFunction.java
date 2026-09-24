/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import reactor.core.publisher.Mono;

/**
 * A three-argument handler function: request context, the typed request, and a notifier
 * to push request-scoped notifications before completing.
 *
 * @author Dariusz Jędrzejczyk
 */
@FunctionalInterface
public interface AsyncStreamingFunction<REQ, RES> {

	Mono<RES> apply(McpRequestContext ctx, REQ request, McpAsyncNotifier notifier);

}
