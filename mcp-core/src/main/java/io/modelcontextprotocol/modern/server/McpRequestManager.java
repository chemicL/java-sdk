/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import reactor.core.publisher.Mono;

/**
 * The transport-facing entry point. Transports depend only on this interface, never on
 * {@link McpServer} directly, so a caller can substitute its own implementation if the
 * composition root is unwanted.
 * <p>
 * A transport resolves each request through the entry point matching how it will consume
 * the result. That choice, not the programming model of the handlers, decides where sync
 * user code runs:
 * <ul>
 * <li>{@link #resolveBlocking} - the calling thread blocks until the invocation
 * completes, so sync repositories and handlers run on it. Thread-locals it carries (a
 * servlet filter's security context, MDC, ...) stay visible to them.
 * <li>{@link #resolveNonBlocking} - the calling thread must never block (an event loop, a
 * single reader thread), so sync repositories and handlers are moved to
 * {@code Schedulers.boundedElastic()}.
 * </ul>
 * Async repositories and handlers run wherever their own publishers run in both cases.
 * <p>
 * Neither method completes with an error: every failure is represented as a JSON-RPC
 * error response inside the returned invocation.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpRequestManager {

	/**
	 * Resolve the request for a caller that blocks its thread until the returned
	 * invocation completes. Sync user code runs on the thread that subscribes.
	 */
	Mono<McpInvocation> resolveBlocking(McpTransportContext transportContext, JSONRPCRequest request);

	/**
	 * Resolve the request for a caller whose thread must never block. Sync user code runs
	 * on {@code Schedulers.boundedElastic()}.
	 */
	Mono<McpInvocation> resolveNonBlocking(McpTransportContext transportContext, JSONRPCRequest request);

	/**
	 * Handle a client notification. The only modern client-to-server notification is
	 * {@code notifications/cancelled}, which transports act on directly; this method
	 * exists for symmetry and future extension notifications.
	 */
	Mono<Void> handleNotification(McpTransportContext transportContext, JSONRPCNotification notification);

}
