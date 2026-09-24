/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpOutcome;
import io.modelcontextprotocol.modern.server.McpSchedulers;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import io.modelcontextprotocol.util.ToolsUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class ToolsFeatureTests {

	private static final Implementation SERVER_INFO = Implementation.builder("test-server", "1.0.0").build();

	private static final io.modelcontextprotocol.spec.McpSchema.Tool ECHO_TOOL = io.modelcontextprotocol.spec.McpSchema.Tool
		.builder("echo", ToolsUtils.EMPTY_JSON_SCHEMA)
		.build();

	private static Map<String, Object> meta() {
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		return meta;
	}

	private static McpServer.Builder baseBuilder() {
		return McpServer.builder().serverInfo(SERVER_INFO).jsonMapper(new GsonMcpJsonMapper());
	}

	@Test
	void unknownToolIsRejected() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of()));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(io.modelcontextprotocol.modern.server.McpRequestContext ctx,
					String name) {
				return Mono.empty();
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "does-not-exist"));

		StepVerifier
			.create(server.resolve(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Unary) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS))
			.verifyComplete();
	}

	@Test
	void asyncUnaryHandlerAnswersAsUnary() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of(ECHO_TOOL)));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(io.modelcontextprotocol.modern.server.McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncToolHandler.of((c,
						req) -> Mono.just(io.modelcontextprotocol.modern.McpSchema.CallToolResult.builder()
							.addContent(new TextContent("echo:" + req.name()))
							.build())));
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "echo"));

		var invocation = server.resolve(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Unary.class);
	}

	@Test
	void streamingHandlerAnswersAsStreaming() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of(ECHO_TOOL)));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(io.modelcontextprotocol.modern.server.McpRequestContext ctx,
					String name) {
				return Mono.just(AsyncToolHandler.streaming((c, req, notifier) -> notifier.progress(1.0, 1.0, "done")
					.thenReturn(io.modelcontextprotocol.modern.McpSchema.CallToolResult.builder()
						.addContent(new TextContent("ok"))
						.build())));
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "echo"));

		var invocation = server.resolve(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Streaming.class);
	}

	@Test
	void syncHandlerRunsOnHandlerScheduler() {
		AtomicReference<String> threadName = new AtomicReference<>();
		McpSyncToolRepository repo = new McpSyncToolRepository() {
			@Override
			public ToolsPage list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return ToolsPage.of(List.of(ECHO_TOOL));
			}

			@Override
			public SyncToolHandler resolve(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String name) {
				return SyncToolHandler.of((c, req) -> {
					threadName.set(Thread.currentThread().getName());
					return io.modelcontextprotocol.modern.McpSchema.CallToolResult.builder()
						.addContent(new TextContent("ok"))
						.build();
				});
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "echo"));

		server.resolve(McpTransportContext.EMPTY, request)
			.flatMap(inv -> ((McpInvocation.Unary) inv).response())
			.contextWrite(c -> c.put(McpSchedulers.HANDLER_SCHEDULER_KEY, Schedulers.immediate()))
			.block();
		assertThat(threadName.get()).isEqualTo(Thread.currentThread().getName());
	}

	@Test
	void outcomeInputRequiredIsPreserved() {
		McpOutcome<io.modelcontextprotocol.modern.McpSchema.CallToolResult> outcome = McpOutcome.inputRequired(
				io.modelcontextprotocol.modern.McpSchema.InputRequiredResult.builder().requestState("s").build());
		assertThat(outcome.isComplete()).isFalse();
		assertThat(outcome.completeResult()).isEmpty();
	}

}
