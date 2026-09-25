/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.feature.AsyncResourceHandler;
import io.modelcontextprotocol.modern.server.feature.AsyncToolHandler;
import io.modelcontextprotocol.modern.server.feature.McpAsyncResourceRepository;
import io.modelcontextprotocol.modern.server.feature.McpAsyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.ResourcesPage;
import io.modelcontextprotocol.modern.server.feature.ToolsPage;
import io.modelcontextprotocol.spec.McpSchema.ElicitFormRequest;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class McpServerMrtrTests {

	private static final Implementation SERVER_INFO = Implementation.builder("test-server", "1.0.0").build();

	private static Map<String, Object> metaWith(Object... extra) {
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		for (int i = 0; i < extra.length; i += 2) {
			meta.put((String) extra[i], extra[i + 1]);
		}
		return meta;
	}

	private static Map<String, Object> metaWithElicitation() {
		Map<String, Object> meta = metaWith();
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of("elicitation", Map.of("form", Map.of())));
		return meta;
	}

	@Test
	void inputRequiredResultSealsRequestStateOnTheWire() {
		AtomicReference<String> seenRequestState = new AtomicReference<>();
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(java.util.List.of()));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(McpRequestContext ctx, String name) {
				return Mono.just(AsyncToolHandler.withInput((c, req) -> {
					if (req.requestState() != null) {
						seenRequestState.set(req.requestState());
						return Mono.just(io.modelcontextprotocol.modern.server.McpRoundResult
							.complete(CallToolResult.builder().addContent(new TextContent("resumed")).build()));
					}
					return Mono.just(io.modelcontextprotocol.modern.server.McpRoundResult
						.inputRequired(InputRequiredResult.builder()
							.elicit("q1", ElicitFormRequest.builder("Confirm?", Map.of("type", "object")).build())
							.requestState("secret-plaintext")
							.build()));
				}));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.tools(repo)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", metaWithElicitation());
		params.put("name", "echo");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, params);

		var response = server.resolve(McpTransportContext.EMPTY, request)
			.flatMap(inv -> ((McpInvocation.Unary) inv).response())
			.block();

		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) response.result();
		String wireRequestState = (String) result.get("requestState");
		assertThat(wireRequestState).isNotNull().isNotEqualTo("secret-plaintext");

		// Retry with the sealed state: the handler must see the plaintext.
		Map<String, Object> retryParams = new java.util.HashMap<>();
		retryParams.put("_meta", metaWithElicitation());
		retryParams.put("name", "echo");
		retryParams.put("requestState", wireRequestState);
		JSONRPCRequest retryRequest = new JSONRPCRequest("tools/call", 2, retryParams);

		var retryResponse = server.resolve(McpTransportContext.EMPTY, retryRequest)
			.flatMap(inv -> ((McpInvocation.Unary) inv).response())
			.block();

		assertThat(retryResponse.error()).isNull();
		assertThat(seenRequestState.get()).isEqualTo("secret-plaintext");
	}

	@Test
	void elicitationWithoutDeclaredCapabilityIsRejected() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(java.util.List.of()));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(McpRequestContext ctx, String name) {
				return Mono.just(AsyncToolHandler.withInput((c, req) -> Mono.just(
						io.modelcontextprotocol.modern.server.McpRoundResult.inputRequired(InputRequiredResult.builder()
							.elicit("q1", ElicitFormRequest.builder("Confirm?", Map.of("type", "object")).build())
							.build()))));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.tools(repo)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", metaWith());
		params.put("name", "echo");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, params);

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code())
				.isEqualTo(ErrorCodes.MISSING_REQUIRED_CLIENT_CAPABILITY))
			.verifyComplete();
	}

	@Test
	void retryResultCarriesNoCacheHints() {
		McpAsyncResourceRepository repo = new McpAsyncResourceRepository() {
			@Override
			public Mono<ResourcesPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ResourcesPage.of(java.util.List.of()));
			}

			@Override
			public Mono<AsyncResourceHandler> resolve(McpRequestContext ctx, String uri) {
				return Mono.just(
						AsyncResourceHandler.of((c,
								req) -> Mono.just(ReadResourceResult.builder(java.util.List
									.of(new io.modelcontextprotocol.spec.McpSchema.TextResourceContents(req.uri(),
											"text/plain", "content", null)))
									.build())));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.resources(repo)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", metaWith());
		params.put("uri", "file:///a.txt");
		params.put("inputResponses", Map.of("q1", Map.of("action", "accept")));
		JSONRPCRequest request = new JSONRPCRequest("resources/read", 1, params);

		var response = server.resolve(McpTransportContext.EMPTY, request)
			.flatMap(inv -> ((McpInvocation.Unary) inv).response())
			.block();

		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) response.result();
		assertThat(result).doesNotContainKeys("ttlMs", "cacheScope");
	}

}
