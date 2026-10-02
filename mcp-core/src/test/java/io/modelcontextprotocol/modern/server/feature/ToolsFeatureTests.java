/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.Implementation;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.TextContent;
import io.modelcontextprotocol.modern.McpSchema.Tool;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import io.modelcontextprotocol.util.ToolsUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class ToolsFeatureTests {

	private static final Implementation SERVER_INFO = Implementation.builder("test-server", "1.0.0").build();

	private static final Tool ECHO_TOOL = Tool.builder("echo", ToolsUtils.EMPTY_JSON_SCHEMA).build();

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
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(
					io.modelcontextprotocol.modern.server.McpRequestContext ctx, String name) {
				return Mono.empty();
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "does-not-exist"));

		StepVerifier
			.create(server.resolveNonBlocking(McpTransportContext.EMPTY, request)
				.flatMap(inv -> ((McpInvocation.Single) inv).response()))
			.assertNext(response -> assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS))
			.verifyComplete();
	}

	@Test
	void asyncSingleHandlerAnswersAsSingle() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of(ECHO_TOOL)));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(
					io.modelcontextprotocol.modern.server.McpRequestContext ctx, String name) {
				return Mono.just(AsyncFeatureHandler.of((c,
						req) -> Mono.just(io.modelcontextprotocol.modern.McpSchema.CallToolResult.builder()
							.addContent(TextContent.builder("echo:" + req.name()).build())
							.build())));
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "echo"));

		var invocation = server.resolveNonBlocking(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Single.class);
	}

	@Test
	void streamingHandlerAnswersAsStreaming() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of(ECHO_TOOL)));
			}

			@Override
			public Mono<AsyncFeatureHandler<CallToolRequest, CallToolResult>> resolve(
					io.modelcontextprotocol.modern.server.McpRequestContext ctx, String name) {
				return Mono.just(AsyncFeatureHandler.streaming((c, req, notifier) -> notifier.progress(1.0, 1.0, "done")
					.thenReturn(io.modelcontextprotocol.modern.McpSchema.CallToolResult.builder()
						.addContent(TextContent.builder("ok").build())
						.build())));
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest request = new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta(), "name", "echo"));

		var invocation = server.resolveNonBlocking(McpTransportContext.EMPTY, request).block();
		assertThat(invocation).isInstanceOf(McpInvocation.Streaming.class);
	}

	private static final ThreadLocal<String> PRINCIPAL = new ThreadLocal<>();

	private static final Tool ADMIN_TOOL = Tool.builder("admin", ToolsUtils.EMPTY_JSON_SCHEMA).build();

	private static JSONRPCRequest callEcho() {
		Map<String, Object> meta = meta();
		meta.put(MetaKeys.PROGRESS_TOKEN, "t1");
		return new JSONRPCRequest("tools/call", 1, Map.of("_meta", meta, "name", "echo"));
	}

	private static CallToolResult greeting(String principal) {
		return CallToolResult.builder().addContent(TextContent.builder("hello " + principal).build()).build();
	}

	@SuppressWarnings("unchecked")
	private static String greetingText(JSONRPCMessage message) {
		Map<String, Object> result = (Map<String, Object>) ((JSONRPCResponse) message).result();
		List<Map<String, Object>> content = (List<Map<String, Object>>) result.get("content");
		return (String) content.get(0).get("text");
	}

	private static McpSyncToolRepository singleGreetingRepo(Function<McpRequestContext, String> principal,
			AtomicReference<String> handlerThread) {
		return new McpSyncToolRepository() {
			@Override
			public ToolsPage list(McpRequestContext ctx, String cursor) {
				return ToolsPage.of(List.of(ECHO_TOOL));
			}

			@Override
			public SyncFeatureHandler<CallToolRequest, CallToolResult> resolve(McpRequestContext ctx, String name) {
				return SyncFeatureHandler.of((c, req) -> {
					handlerThread.set(Thread.currentThread().getName());
					return greeting(principal.apply(c));
				});
			}
		};
	}

	/**
	 * Records how many messages the consumer had already received when the handler's
	 * progress call returned, to tell inline delivery apart from buffering.
	 */
	private static McpSyncToolRepository streamingGreetingRepo(Function<McpRequestContext, String> principal,
			AtomicReference<String> handlerThread, List<JSONRPCMessage> delivered,
			AtomicInteger deliveredWhenProgressReturned) {
		return new McpSyncToolRepository() {
			@Override
			public ToolsPage list(McpRequestContext ctx, String cursor) {
				return ToolsPage.of(List.of(ECHO_TOOL));
			}

			@Override
			public SyncFeatureHandler<CallToolRequest, CallToolResult> resolve(McpRequestContext ctx, String name) {
				return SyncFeatureHandler.streaming((c, req, notifier) -> {
					handlerThread.set(Thread.currentThread().getName());
					notifier.progress(1.0, 1.0, "greeting");
					deliveredWhenProgressReturned.set(delivered.size());
					return greeting(principal.apply(c));
				});
			}
		};
	}

	private static JSONRPCResponse callSingle(Mono<McpInvocation> invocation) {
		return invocation.flatMap(inv -> ((McpInvocation.Single) inv).response()).block();
	}

	private static List<JSONRPCMessage> callStreaming(Mono<McpInvocation> invocation, List<JSONRPCMessage> delivered) {
		((McpInvocation.Streaming) invocation.block()).messages().doOnNext(delivered::add).blockLast();
		return delivered;
	}

	@Test
	void blockingCallerRunsSyncSingleHandlerOnItsOwnThread() {
		AtomicReference<String> handlerThread = new AtomicReference<>();
		McpServer server = baseBuilder().tools(singleGreetingRepo(c -> PRINCIPAL.get(), handlerThread)).build();

		PRINCIPAL.set("alice");
		JSONRPCResponse response;
		try {
			response = callSingle(server.resolveBlocking(McpTransportContext.EMPTY, callEcho()));
		}
		finally {
			PRINCIPAL.remove();
		}

		assertThat(handlerThread.get()).isEqualTo(Thread.currentThread().getName());
		assertThat(greetingText(response)).isEqualTo("hello alice");
	}

	@Test
	void blockingCallerRunsSyncStreamingHandlerOnItsOwnThreadAndReceivesNotificationsInline() {
		AtomicReference<String> handlerThread = new AtomicReference<>();
		List<JSONRPCMessage> delivered = new CopyOnWriteArrayList<>();
		AtomicInteger deliveredWhenProgressReturned = new AtomicInteger(-1);
		McpServer server = baseBuilder()
			.tools(streamingGreetingRepo(c -> PRINCIPAL.get(), handlerThread, delivered, deliveredWhenProgressReturned))
			.build();

		PRINCIPAL.set("alice");
		try {
			callStreaming(server.resolveBlocking(McpTransportContext.EMPTY, callEcho()), delivered);
		}
		finally {
			PRINCIPAL.remove();
		}

		assertThat(handlerThread.get()).isEqualTo(Thread.currentThread().getName());
		assertThat(deliveredWhenProgressReturned.get()).isEqualTo(1);
		assertThat(delivered).hasSize(2);
		assertThat(delivered.get(0)).isInstanceOf(JSONRPCNotification.class);
		assertThat(greetingText(delivered.get(1))).isEqualTo("hello alice");
	}

	@Test
	@SuppressWarnings("unchecked")
	void blockingCallerRunsSyncListOnItsOwnThread() {
		McpSyncToolRepository repo = new McpSyncToolRepository() {
			@Override
			public ToolsPage list(McpRequestContext ctx, String cursor) {
				return ToolsPage
					.of("admin".equals(PRINCIPAL.get()) ? List.of(ECHO_TOOL, ADMIN_TOOL) : List.of(ECHO_TOOL));
			}

			@Override
			public SyncFeatureHandler<CallToolRequest, CallToolResult> resolve(McpRequestContext ctx, String name) {
				return null;
			}
		};
		McpServer server = baseBuilder().tools(repo).build();
		JSONRPCRequest listTools = new JSONRPCRequest("tools/list", 1, Map.of("_meta", meta()));

		PRINCIPAL.set("admin");
		JSONRPCResponse asAdmin;
		JSONRPCResponse asAnonymous;
		try {
			asAdmin = callSingle(server.resolveBlocking(McpTransportContext.EMPTY, listTools));
			PRINCIPAL.remove();
			asAnonymous = callSingle(server.resolveBlocking(McpTransportContext.EMPTY, listTools));
		}
		finally {
			PRINCIPAL.remove();
		}

		assertThat((List<Object>) ((Map<String, Object>) asAdmin.result()).get("tools")).hasSize(2);
		assertThat((List<Object>) ((Map<String, Object>) asAnonymous.result()).get("tools")).hasSize(1);
	}

	@Test
	void nonBlockingCallerOffloadsSyncSingleHandler() {
		AtomicReference<String> handlerThread = new AtomicReference<>();
		McpServer server = baseBuilder().tools(singleGreetingRepo(c -> PRINCIPAL.get(), handlerThread)).build();

		PRINCIPAL.set("alice");
		JSONRPCResponse response;
		try {
			response = callSingle(server.resolveNonBlocking(McpTransportContext.EMPTY, callEcho()));
		}
		finally {
			PRINCIPAL.remove();
		}

		assertThat(handlerThread.get()).startsWith("boundedElastic-");
		assertThat(greetingText(response)).isEqualTo("hello null");
	}

	@Test
	void nonBlockingCallerOffloadsSyncStreamingHandler() {
		AtomicReference<String> handlerThread = new AtomicReference<>();
		List<JSONRPCMessage> delivered = new CopyOnWriteArrayList<>();
		McpServer server = baseBuilder()
			.tools(streamingGreetingRepo(c -> PRINCIPAL.get(), handlerThread, delivered, new AtomicInteger()))
			.build();

		PRINCIPAL.set("alice");
		try {
			callStreaming(server.resolveNonBlocking(McpTransportContext.EMPTY, callEcho()), delivered);
		}
		finally {
			PRINCIPAL.remove();
		}

		assertThat(handlerThread.get()).startsWith("boundedElastic-");
		assertThat(delivered).hasSize(2);
		assertThat(greetingText(delivered.get(1))).isEqualTo("hello null");
	}

	@Test
	void offloadedSyncHandlerSeesPrincipalCapturedInTransportContext() {
		AtomicReference<String> handlerThread = new AtomicReference<>();
		McpServer server = baseBuilder()
			.tools(singleGreetingRepo(c -> (String) c.transportContext().get("principal"), handlerThread))
			.build();
		McpTransportContext transportContext = McpTransportContext.create(Map.of("principal", "alice"));

		JSONRPCResponse response = callSingle(server.resolveNonBlocking(transportContext, callEcho()));

		assertThat(handlerThread.get()).startsWith("boundedElastic-");
		assertThat(greetingText(response)).isEqualTo("hello alice");
	}

	@Test
	void withInputHandlerCanAnswerInputRequired() {
		io.modelcontextprotocol.modern.McpSchema.InputRequiredResult inputRequired = io.modelcontextprotocol.modern.McpSchema.InputRequiredResult
			.builder()
			.requestState("s")
			.build();
		AsyncFeatureHandler<CallToolRequest, CallToolResult> handler = AsyncFeatureHandler
			.withInput((ctx, req) -> Mono.just(McpRoundResult.inputRequired(inputRequired)));

		StepVerifier.create(handler.handle(null, null)).assertNext(round -> {
			assertThat(round).isInstanceOf(McpRoundResult.InputRequired.class);
			assertThat(round.result()).isSameAs(inputRequired);
		}).verifyComplete();
	}

}
