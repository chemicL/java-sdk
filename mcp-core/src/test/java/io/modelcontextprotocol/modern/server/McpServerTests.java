/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.McpSchema.TextContent;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static io.modelcontextprotocol.modern.server.ModernTestFixtures.SERVER_INFO;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.meta;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.respond;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class McpServerTests {

	private static McpServer.Builder baseBuilder() {
		return McpServer.builder().serverInfo(SERVER_INFO).jsonMapper(new GsonMcpJsonMapper());
	}

	private static McpFeature feature(String method, McpHandler handler) {
		return new McpFeature() {
			@Override
			public Set<String> methods() {
				return Set.of(method);
			}

			@Override
			public Mono<McpHandler> resolve(McpRequestContext ctx) {
				return Mono.just(handler);
			}
		};
	}

	private static McpFeature echoFeature(String method) {
		return feature(method, (ctx, params) -> Mono
			.just(CallToolResult.builder().addContent(TextContent.builder("ok").build()).build()));
	}

	@Test
	void missingMetaIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1, Map.of());

		StepVerifier.create(respond(server, request)).assertNext(response -> {
			assertThat(response.error()).isNotNull();
			assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS);
		}).verifyComplete();
	}

	@Test
	void missingClientCapabilitiesIsRejected() {
		McpServer server = baseBuilder().build();
		Map<String, Object> meta = new HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, McpSchema.LATEST_PROTOCOL_VERSION);
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1, Map.of("_meta", meta));

		StepVerifier.create(respond(server, request))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS))
			.verifyComplete();
	}

	@Test
	void unsupportedVersionIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1,
				Map.of("_meta", meta(MetaKeys.PROTOCOL_VERSION, "1999-01-01")));

		StepVerifier.create(respond(server, request))
			.assertNext(
					response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION))
			.verifyComplete();
	}

	@Test
	void unknownMethodIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("does/not/exist", 1, Map.of("_meta", meta()));

		StepVerifier.create(respond(server, request))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.METHOD_NOT_FOUND))
			.verifyComplete();
	}

	@Test
	void handlerExceptionBecomesInternalError() {
		McpFeature failing = feature("tools/call", (ctx, params) -> Mono.error(new RuntimeException("boom")));
		McpServer server = baseBuilder().feature(failing).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta()));

		StepVerifier.create(respond(server, request))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INTERNAL_ERROR))
			.verifyComplete();
	}

	@Test
	void serverInfoIsStampedOnEveryResult() {
		McpServer server = baseBuilder().feature(echoFeature("tools/call")).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta()));

		StepVerifier.create(respond(server, request)).assertNext(response -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) response.result();
			@SuppressWarnings("unchecked")
			Map<String, Object> meta = (Map<String, Object>) result.get("_meta");
			assertThat(meta).containsKey(MetaKeys.SERVER_INFO);
		}).verifyComplete();
	}

	@Test
	void discoverReturnsAggregatedCapabilities() {
		McpFeature toolsCapability = new McpFeature() {
			@Override
			public Set<String> methods() {
				return Set.of();
			}

			@Override
			public Mono<McpHandler> resolve(McpRequestContext ctx) {
				return Mono.empty();
			}

			@Override
			public void capabilities(ServerCapabilities.Builder builder) {
				builder.tools(false);
			}
		};
		McpServer server = baseBuilder().feature(toolsCapability).build();
		JSONRPCRequest request = new JSONRPCRequest("server/discover", 1, Map.of("_meta", meta()));

		StepVerifier.create(respond(server, request)).assertNext(response -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) response.result();
			assertThat(result.get("supportedVersions")).isEqualTo(List.of(McpSchema.LATEST_PROTOCOL_VERSION));
			assertThat(result.get("capabilities")).isNotNull();
		}).verifyComplete();
	}

	@Test
	void streamingHandlerYieldsStreamingInvocation() {
		McpHandler.Streaming streaming = new McpHandler.Streaming() {
			@Override
			public Mono<Result> handle(McpRequestContext ctx, Object params, McpAsyncNotifier notifier) {
				return notifier.progress(1.0, 2.0, "half")
					.then(notifier.progress(2.0, 2.0, "done"))
					.then(Mono.just(CallToolResult.builder().addContent(TextContent.builder("ok").build()).build()));
			}
		};
		McpFeature feature = feature("tools/call", streaming);
		McpServer server = baseBuilder().feature(feature).build();

		Map<String, Object> meta = meta(MetaKeys.PROGRESS_TOKEN, "tok-1");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta));

		var invocation = server.resolveNonBlocking(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Streaming.class);

		StepVerifier.create(((McpInvocation.Streaming) invocation).messages())
			.expectNextCount(2)
			.expectNextMatches(msg -> msg instanceof JSONRPCResponse)
			.verifyComplete();
	}

	@Test
	void progressIsSuppressedWithoutProgressToken() {
		McpHandler.Streaming streaming = new McpHandler.Streaming() {
			@Override
			public Mono<Result> handle(McpRequestContext ctx, Object params, McpAsyncNotifier notifier) {
				return notifier.progress(1.0, null, null)
					.then(Mono.just(CallToolResult.builder().addContent(TextContent.builder("ok").build()).build()));
			}
		};
		McpFeature feature = feature("tools/call", streaming);
		McpServer server = baseBuilder().feature(feature).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta()));

		var invocation = (McpInvocation.Streaming) server.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.block();
		StepVerifier.create(invocation.messages())
			.expectNextMatches(msg -> msg instanceof JSONRPCResponse)
			.verifyComplete();
	}

	@Test
	void inputRequiredFromDisallowedMethodBecomesInternalError() {
		McpFeature feature = feature("resources/list",
				(ctx, params) -> Mono.just(InputRequiredResult.builder().requestState("s").build()));
		McpServer server = baseBuilder().feature(feature).build();
		JSONRPCRequest request = new JSONRPCRequest("resources/list", 1, Map.of("_meta", meta()));

		StepVerifier.create(respond(server, request))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INTERNAL_ERROR))
			.verifyComplete();
	}

	@Test
	void methodServedByTwoFeaturesIsRejectedAtBuild() {
		McpServer.Builder builder = baseBuilder().feature(echoFeature("tools/call")).feature(echoFeature("tools/call"));

		assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("tools/call");
	}

	@Test
	void featureCannotClaimDiscover() {
		McpServer.Builder builder = baseBuilder().feature(echoFeature("server/discover"));

		assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("server/discover");
	}

}
