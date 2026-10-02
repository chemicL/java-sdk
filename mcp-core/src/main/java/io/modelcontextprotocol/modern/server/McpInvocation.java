/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * What a transport gets back from {@link McpRequestManager}: either a single response
 * ({@link Single}, answered as {@code application/json}) or a message stream
 * ({@link Streaming}, answered as {@code text/event-stream}) whose last element is always
 * the terminal {@link JSONRPCResponse}.
 * <p>
 * Deliberately not a {@code sealed} type: the two kinds are known and closed today, but
 * the point of keeping this open is that a transport only needs {@code instanceof} checks
 * against the two public nested classes, not a switch that the compiler pins to an
 * exhaustive set.
 *
 * @author Dariusz Jędrzejczyk
 */
public abstract class McpInvocation {

	private McpInvocation() {
	}

	public static Single single(Mono<JSONRPCResponse> response) {
		return new Single(response);
	}

	public static Streaming streaming(Flux<JSONRPCMessage> messages) {
		return new Streaming(messages);
	}

	/** Answer with a single {@code application/json} response. */
	public static final class Single extends McpInvocation {

		private final Mono<JSONRPCResponse> response;

		private Single(Mono<JSONRPCResponse> response) {
			this.response = response;
		}

		public Mono<JSONRPCResponse> response() {
			return this.response;
		}

	}

	/**
	 * Answer with a {@code text/event-stream}: zero or more notifications followed by
	 * exactly one terminal {@link JSONRPCResponse}.
	 */
	public static final class Streaming extends McpInvocation {

		private final Flux<JSONRPCMessage> messages;

		private Streaming(Flux<JSONRPCMessage> messages) {
			this.messages = messages;
		}

		public Flux<JSONRPCMessage> messages() {
			return this.messages;
		}

	}

}
