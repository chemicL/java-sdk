/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import reactor.core.publisher.Mono;

/**
 * The transport-facing entry point that resolves each request to an
 * {@link McpInvocation}. Use {@link #resolveBlocking} when the calling thread may block,
 * {@link #resolveNonBlocking} when it must not. Neither completes with an error: failures
 * are JSON-RPC error responses inside the invocation.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpRequestManager {

	/**
	 * Resolves a request for a caller that blocks until the invocation completes. Sync
	 * user code runs on the subscribing thread.
	 */
	Mono<McpInvocation> resolveBlocking(McpTransportContext transportContext, JSONRPCRequest request);

	/**
	 * Resolves a request for a caller that must never block. Sync user code runs on
	 * {@code Schedulers.boundedElastic()}.
	 */
	Mono<McpInvocation> resolveNonBlocking(McpTransportContext transportContext, JSONRPCRequest request);

	/**
	 * Handles a client notification. The only one defined is
	 * {@code notifications/cancelled}, which transports act on themselves.
	 */
	Mono<Void> handleNotification(McpTransportContext transportContext, JSONRPCNotification notification);

}
