/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import reactor.core.publisher.Mono;

/**
 * Wraps route resolution, e.g. for authorization, tracing, or decorating a resolved
 * handler (a tasks extension wrapping {@code tools/call} is a filter, not a feature).
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpFilter {

	Mono<McpHandler> filter(McpRequestContext ctx, McpRouter next);

}
