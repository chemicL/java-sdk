/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ElicitFormRequest;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.InputRequest;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.McpSchema.TextContent;
import io.modelcontextprotocol.modern.McpSchema.TextResourceContents;
import io.modelcontextprotocol.modern.server.feature.AsyncFeatureHandler;
import io.modelcontextprotocol.modern.server.feature.McpAsyncResourceRepository;
import io.modelcontextprotocol.modern.server.feature.McpAsyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.ResourcesFeature;
import io.modelcontextprotocol.modern.server.feature.ResourcesPage;
import io.modelcontextprotocol.modern.server.feature.ToolsFeature;
import io.modelcontextprotocol.modern.server.feature.ToolsPage;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static io.modelcontextprotocol.modern.server.McpRoundResult.*;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.SERVER_INFO;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.meta;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.respond;
import static org.assertj.core.api.Assertions.assertThat;

class McpServerMrtrTests {

	private static Map<String, Object> metaWithElicitation() {
		Map<String, Object> meta = meta();
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of("elicitation", Map.of("form", Map.of())));
		return meta;
	}

	@Test
	void inputRequiredResultSealsRequestStateOnTheWire() {
		AtomicReference<String> seenRequestState = new AtomicReference<>();
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncFeatureHandler.withInput((c, req) -> {
					if (req.requestState() != null) {
						seenRequestState.set(req.requestState());
						return Mono.just(McpRoundResult.complete(
								CallToolResult.builder().addContent(TextContent.builder("resumed").build()).build()));
					}
					return Mono.just(inputRequired(InputRequiredResult.builder()
						.elicit("q1", ElicitFormRequest.builder("Confirm?", Map.of("type", "object")).build())
						.requestState("secret-plaintext")
						.build()));
				}));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ToolsFeature.of(repo, new GsonMcpJsonMapper(), 0L, CacheScope.PRIVATE))
			.build();

		Map<String, Object> params = new HashMap<>();
		params.put("_meta", metaWithElicitation());
		params.put("name", "echo");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, params);

		var response = respond(server, request).block();

		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) response.result();
		String wireRequestState = (String) result.get("requestState");
		assertThat(wireRequestState).isNotNull().isNotEqualTo("secret-plaintext");

		// Retry with the sealed state: the handler must see the plaintext.
		Map<String, Object> retryParams = new HashMap<>();
		retryParams.put("_meta", metaWithElicitation());
		retryParams.put("name", "echo");
		retryParams.put("requestState", wireRequestState);
		JSONRPCRequest retryRequest = new JSONRPCRequest("tools/call", 2, retryParams);

		var retryResponse = respond(server, retryRequest).block();

		assertThat(retryResponse.error()).isNull();
		assertThat(seenRequestState.get()).isEqualTo("secret-plaintext");
	}

	@Test
	void nonStringRequestStateIsRejected() {
		AtomicReference<String> seenRequestState = new AtomicReference<>();
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncFeatureHandler.withInput((c, req) -> {
					seenRequestState.set(req.requestState());
					return Mono.just(McpRoundResult.complete(CallToolResult.builder().build()));
				}));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ToolsFeature.of(repo, new GsonMcpJsonMapper(), 0L, CacheScope.PRIVATE))
			.build();

		Map<String, Object> params = new HashMap<>();
		params.put("_meta", metaWithElicitation());
		params.put("name", "echo");
		params.put("requestState", 42);
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, params);

		var response = respond(server, request).block();

		assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS);
		assertThat(seenRequestState.get()).isNull();
	}

	private static McpServer serverAnswering(InputRequiredResult inputRequired) {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncFeatureHandler.withInput((c, req) -> Mono.just(inputRequired(inputRequired))));
			}
		};
		return McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ToolsFeature.of(repo, new GsonMcpJsonMapper(), 0L, CacheScope.PRIVATE))
			.build();
	}

	private static JSONRPCRequest toolCall(Map<String, Object> meta) {
		Map<String, Object> params = new HashMap<>();
		params.put("_meta", meta);
		params.put("name", "echo");
		return new JSONRPCRequest("tools/call", 1, params);
	}

	@Test
	void inputRequestWithUnknownMethodIsInternalError() {
		McpServer server = serverAnswering(
				new InputRequiredResult(Map.of("q1", new InputRequest("tasks/get", Map.of())), null, null, null));

		var response = respond(server, toolCall(metaWithElicitation())).block();

		assertThat(response.error().code()).isEqualTo(ErrorCodes.INTERNAL_ERROR);
	}

	@Test
	void rawMapUrlElicitationRequiresUrlCapability() {
		InputRequest urlElicitation = new InputRequest(McpSchema.METHOD_ELICITATION_CREATE,
				Map.of("mode", "url", "message", "Sign in", "url", "https://example.com/login"));
		McpServer server = serverAnswering(new InputRequiredResult(Map.of("q1", urlElicitation), null, null, null));

		// The client declares form-mode elicitation only.
		var response = respond(server, toolCall(metaWithElicitation())).block();

		assertThat(response.error().code()).isEqualTo(ErrorCodes.MISSING_REQUIRED_CLIENT_CAPABILITY);
	}

	@Test
	void elicitationWithoutDeclaredCapabilityIsRejected() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncFeatureHandler.withInput((c,
						req) -> Mono.just(inputRequired(InputRequiredResult.builder()
							.elicit("q1", ElicitFormRequest.builder("Confirm?", Map.of("type", "object")).build())
							.build()))));
				// FIXME: This paradigm can fail because you could do this:
				// return Mono.just(AsyncFeatureHandler.withInput((c, req) ->
				// Mono.just(new
				// McpRoundResult<CallToolResult>() {
				// @Override
				// public McpSchema.Result result() {
				// return new McpSchema.GetPromptResult(null, null, null, null);
				// }
				// })));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ToolsFeature.of(repo, new GsonMcpJsonMapper(), 0L, CacheScope.PRIVATE))
			.build();

		Map<String, Object> params = new HashMap<>();
		params.put("_meta", meta());
		params.put("name", "echo");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, params);

		StepVerifier.create(respond(server, request))
			.assertNext(response -> assertThat(response.error().code())
				.isEqualTo(ErrorCodes.MISSING_REQUIRED_CLIENT_CAPABILITY))
			.verifyComplete();
	}

	@Test
	void retryResultIsMarkedUncacheable() {
		McpAsyncResourceRepository repo = new McpAsyncResourceRepository() {
			@Override
			public Mono<ResourcesPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ResourcesPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<ReadResourceRequest, ReadResourceResult>> resolve(McpRequestContext ctx,
					String uri) {
				return Mono.just(AsyncFeatureHandler.of((c,
						req) -> Mono.just(ReadResourceResult
							.builder(List.of(new TextResourceContents(req.uri(), "text/plain", "content", null)))
							.ttlMs(60_000L)
							.cacheScope(CacheScope.PUBLIC)
							.build())));
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ResourcesFeature.of(repo, new GsonMcpJsonMapper(), 0L, CacheScope.PRIVATE))
			.build();

		Map<String, Object> params = new HashMap<>();
		params.put("_meta", meta());
		params.put("uri", "file:///a.txt");
		params.put("inputResponses", Map.of("q1", Map.of("action", "accept")));
		JSONRPCRequest request = new JSONRPCRequest("resources/read", 1, params);

		var response = respond(server, request).block();

		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) response.result();
		assertThat(((Number) result.get("ttlMs")).longValue()).isZero();
		// Gson ignores @JsonProperty on enums, so compare case-insensitively.
		assertThat((String) result.get("cacheScope")).isEqualToIgnoringCase("private");
	}

}
