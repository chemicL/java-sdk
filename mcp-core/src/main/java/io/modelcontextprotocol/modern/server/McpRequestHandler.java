/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import reactor.core.publisher.Mono;

/**
 * The transport-facing entry point. Transports depend only on this interface, never on
 * {@link McpServer} directly, so a caller can substitute a bare router-backed
 * implementation if the composition root is unwanted.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpRequestHandler {

	/**
	 * Resolve and, for a {@code McpInvocation.Unary}, begin invoking the request. Never
	 * completes with an error - every failure is represented as a JSON-RPC error response
	 * inside the returned invocation.
	 */
	Mono<McpInvocation> resolve(McpTransportContext transportContext, JSONRPCRequest request);

	/**
	 * Handle a client notification. The only modern client-to-server notification is
	 * {@code notifications/cancelled}, which transports act on directly; this method
	 * exists for symmetry and future extension notifications.
	 */
	Mono<Void> handleNotification(McpTransportContext transportContext, JSONRPCNotification notification);

}
