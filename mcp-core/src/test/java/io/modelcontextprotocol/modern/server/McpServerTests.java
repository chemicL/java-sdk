/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class McpServerTests {

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

	private static McpServer.Builder baseBuilder() {
		return McpServer.builder().serverInfo(SERVER_INFO).jsonMapper(new GsonMcpJsonMapper());
	}

	private static McpFeature echoFeature(String method) {
		return () -> ctx -> {
			if (!method.equals(ctx.method())) {
				return Mono.empty();
			}
			McpHandler handler = (c,
					params) -> Mono.just(CallToolResult.builder()
						.addContent(new io.modelcontextprotocol.spec.McpSchema.TextContent("ok"))
						.build());
			return Mono.just(handler);
		};
	}

	@Test
	void missingMetaIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1, Map.of());

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> {
				assertThat(response.error()).isNotNull();
				assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS);
			})
			.verifyComplete();
	}

	@Test
	void missingClientCapabilitiesIsRejected() {
		McpServer server = baseBuilder().build();
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION);
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1, Map.of("_meta", meta));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS))
			.verifyComplete();
	}

	@Test
	void unsupportedVersionIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("tools/list", 1,
				Map.of("_meta", metaWith(MetaKeys.PROTOCOL_VERSION, "1999-01-01")));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(
					response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION))
			.verifyComplete();
	}

	@Test
	void unknownMethodIsRejected() {
		McpServer server = baseBuilder().build();
		JSONRPCRequest request = new JSONRPCRequest("does/not/exist", 1, Map.of("_meta", metaWith()));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.METHOD_NOT_FOUND))
			.verifyComplete();
	}

	@Test
	void handlerExceptionBecomesInternalError() {
		McpFeature failing = () -> ctx -> Mono
			.just((McpHandler) (c, params) -> Mono.error(new RuntimeException("boom")));
		McpServer server = baseBuilder().feature(failing).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", metaWith()));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INTERNAL_ERROR))
			.verifyComplete();
	}

	@Test
	void serverInfoIsStampedOnEveryResult() {
		McpServer server = baseBuilder().feature(echoFeature("tools/call")).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", metaWith()));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> {
				@SuppressWarnings("unchecked")
				Map<String, Object> result = (Map<String, Object>) response.result();
				@SuppressWarnings("unchecked")
				Map<String, Object> meta = (Map<String, Object>) result.get("_meta");
				assertThat(meta).containsKey(MetaKeys.SERVER_INFO);
			})
			.verifyComplete();
	}

	@Test
	void discoverReturnsAggregatedCapabilities() {
		McpFeature toolsCapability = new McpFeature() {
			@Override
			public McpRouter router() {
				return McpRouter.empty();
			}

			@Override
			public void capabilities(ServerCapabilities.Builder builder) {
				builder.tools(false);
			}
		};
		McpServer server = baseBuilder().feature(toolsCapability).build();
		JSONRPCRequest request = new JSONRPCRequest("server/discover", 1, Map.of("_meta", metaWith()));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> {
				@SuppressWarnings("unchecked")
				Map<String, Object> result = (Map<String, Object>) response.result();
				assertThat(result.get("supportedVersions"))
					.isEqualTo(List.of(io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION));
				assertThat(result.get("capabilities")).isNotNull();
			})
			.verifyComplete();
	}

	@Test
	void streamingHandlerYieldsStreamingInvocation() {
		McpHandler.Streaming streaming = new McpHandler.Streaming() {
			@Override
			public Mono<Result> handle(McpRequestContext ctx, Object params, McpAsyncNotifier notifier) {
				return notifier.progress(1.0, 2.0, "half")
					.then(notifier.progress(2.0, 2.0, "done"))
					.then(Mono.just(CallToolResult.builder()
						.addContent(new io.modelcontextprotocol.spec.McpSchema.TextContent("ok"))
						.build()));
			}
		};
		McpFeature feature = () -> ctx -> "tools/call".equals(ctx.method()) ? Mono.just(streaming) : Mono.empty();
		McpServer server = baseBuilder().feature(feature).build();

		Map<String, Object> meta = metaWith(MetaKeys.PROGRESS_TOKEN, "tok-1");
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta));

		var invocation = server.resolve(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Streaming.class);

		StepVerifier.create(((McpInvocation.Streaming) invocation).messages())
			.expectNextCount(2)
			.expectNextMatches(msg -> msg instanceof io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse)
			.verifyComplete();
	}

	@Test
	void progressIsSuppressedWithoutProgressToken() {
		McpHandler.Streaming streaming = new McpHandler.Streaming() {
			@Override
			public Mono<Result> handle(McpRequestContext ctx, Object params, McpAsyncNotifier notifier) {
				return notifier.progress(1.0, null, null)
					.then(Mono.just(CallToolResult.builder()
						.addContent(new io.modelcontextprotocol.spec.McpSchema.TextContent("ok"))
						.build()));
			}
		};
		McpFeature feature = () -> ctx -> Mono.just(streaming);
		McpServer server = baseBuilder().feature(feature).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", metaWith()));

		var invocation = (McpInvocation.Streaming) server.resolve(McpTransportContext.EMPTY, request).block();
		StepVerifier.create(invocation.messages())
			.expectNextMatches(msg -> msg instanceof io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse)
			.verifyComplete();
	}

	@Test
	void inputRequiredFromDisallowedMethodBecomesInternalError() {
		McpFeature feature = () -> ctx -> Mono
			.just((McpHandler) (c, params) -> Mono.just(InputRequiredResult.builder().requestState("s").build()));
		McpServer server = baseBuilder().feature(feature).build();
		JSONRPCRequest request = new JSONRPCRequest("resources/list", 1, Map.of("_meta", metaWith()));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INTERNAL_ERROR))
			.verifyComplete();
	}

}
