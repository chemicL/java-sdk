/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.Implementation;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.feature.AsyncFeatureHandler;
import io.modelcontextprotocol.modern.server.feature.McpAsyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.ToolsPage;
import reactor.core.publisher.Mono;

/**
 * Shared helpers for the modern server tests.
 */
public final class ModernTestFixtures {

	public static final Implementation SERVER_INFO = Implementation.builder("test-server", "1.0.0").build();

	private ModernTestFixtures() {
	}

	/**
	 * A valid {@code _meta} (latest version, no capabilities) plus the given key/value
	 * pairs.
	 */
	public static Map<String, Object> meta(Object... extra) {
		Map<String, Object> meta = new HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		for (int i = 0; i < extra.length; i += 2) {
			meta.put((String) extra[i], extra[i + 1]);
		}
		return meta;
	}

	/** Resolves the request without blocking and returns the single response. */
	public static Mono<JSONRPCResponse> respond(McpRequestManager manager, JSONRPCRequest request) {
		return manager.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.flatMap(invocation -> ((McpInvocation.Single) invocation).response());
	}

	/** A tool repository with no tools. */
	public static McpAsyncToolRepository emptyTools() {
		return new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return Mono.empty();
			}
		};
	}

}
