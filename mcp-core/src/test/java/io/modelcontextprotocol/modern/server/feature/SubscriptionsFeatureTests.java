/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.Implementation;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionsFeatureTests {

	private static final Implementation SERVER_INFO = Implementation.builder("test-server", "1.0.0").build();

	private static Map<String, Object> meta() {
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		return meta;
	}

	private static McpAsyncToolRepository noopTools() {
		return new McpAsyncToolRepository() {
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
	}

	@Test
	void ackIsFirstAndReflectsHonouredSubset() {
		SinkChangeFeed feed = McpChangeFeed.sink();
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.tools(noopTools())
			.subscriptions(feed)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("notifications", Map.of("toolsListChanged", true, "promptsListChanged", true));
		JSONRPCRequest request = new JSONRPCRequest("subscriptions/listen", 7, params);

		var invocation = (McpInvocation.Streaming) server.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.block();

		// The feed has no buffered replay, so emit only after the listen stream has
		// actually subscribed - otherwise the change is dropped before anyone is
		// listening, same as a real client connecting after a change already fired.
		StepVerifier.create(invocation.messages()).assertNext(msg -> {
			JSONRPCNotification ack = (JSONRPCNotification) msg;
			assertThat(ack.method()).isEqualTo(McpSchema.METHOD_NOTIFICATION_SUBSCRIPTIONS_ACKNOWLEDGED);
			@SuppressWarnings("unchecked")
			Map<String, Object> ackParams = (Map<String, Object>) new GsonMcpJsonMapper().convertValue(ack.params(),
					Map.class);
			@SuppressWarnings("unchecked")
			Map<String, Object> notifications = (Map<String, Object>) ackParams.get("notifications");
			assertThat(notifications.get("toolsListChanged")).isEqualTo(true);
			assertThat(notifications.get("promptsListChanged")).isNull();
		})
			.then(() -> feed.emit(new ServerChange.ToolsListChanged()))
			.assertNext(msg -> assertThat(((JSONRPCNotification) msg).method())
				.isEqualTo(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED))
			.then(() -> feed.emit(new ServerChange.PromptsListChanged()))
			.then(server::closeGracefully)
			.assertNext(msg -> assertThat(msg).isInstanceOf(JSONRPCResponse.class))
			.verifyComplete();
	}

	@Test
	void unrequestedTypeIsNeverEmitted() {
		SinkChangeFeed feed = McpChangeFeed.sink();
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.tools(noopTools())
			.subscriptions(feed)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("notifications", Map.of("toolsListChanged", true));
		JSONRPCRequest request = new JSONRPCRequest("subscriptions/listen", 1, params);

		var invocation = (McpInvocation.Streaming) server.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.block();

		StepVerifier.create(invocation.messages())
			.expectNextMatches(msg -> ((JSONRPCNotification) msg).method()
				.equals(McpSchema.METHOD_NOTIFICATION_SUBSCRIPTIONS_ACKNOWLEDGED))
			.then(() -> feed.emit(new ServerChange.PromptsListChanged())) // not requested
			.then(() -> feed.emit(new ServerChange.ToolsListChanged()))
			.expectNextMatches(msg -> ((JSONRPCNotification) msg).method()
				.equals(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED))
			.then(server::closeGracefully)
			.expectNextMatches(msg -> msg instanceof JSONRPCResponse)
			.verifyComplete();
	}

	@Test
	void subscriptionIdMatchesRequestId() {
		SinkChangeFeed feed = McpChangeFeed.sink();
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.tools(noopTools())
			.subscriptions(feed)
			.build();

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("notifications", Map.of("toolsListChanged", true));
		JSONRPCRequest request = new JSONRPCRequest("subscriptions/listen", 42, params);

		var invocation = (McpInvocation.Streaming) server.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.block();
		server.closeGracefully();

		StepVerifier.create(invocation.messages()).assertNext(msg -> {
			JSONRPCNotification ack = (JSONRPCNotification) msg;
			assertThat(subscriptionIdOf(ack)).isEqualTo(42L);
		}).assertNext(msg -> assertThat(msg).isInstanceOf(JSONRPCResponse.class)).verifyComplete();
	}

	@SuppressWarnings("unchecked")
	private static Object subscriptionIdOf(JSONRPCNotification notification) {
		Map<String, Object> params = (Map<String, Object>) new GsonMcpJsonMapper().convertValue(notification.params(),
				Map.class);
		Map<String, Object> meta = (Map<String, Object>) params.get("meta");
		return meta.get(MetaKeys.SUBSCRIPTION_ID);
	}

}
