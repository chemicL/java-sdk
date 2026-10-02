/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class StdioMcpTransportTests {

	private final GsonMcpJsonMapper jsonMapper = new GsonMcpJsonMapper();

	private PipedOutputStream clientOut;

	private PipedInputStream serverIn;

	private PipedOutputStream serverOut;

	private BufferedReader serverResponses;

	private StdioMcpTransport transport;

	private void start(McpRequestManager manager) throws IOException {
		this.clientOut = new PipedOutputStream();
		this.serverIn = new PipedInputStream(this.clientOut);
		PipedInputStream clientIn = new PipedInputStream();
		this.serverOut = new PipedOutputStream(clientIn);
		this.serverResponses = new BufferedReader(new InputStreamReader(clientIn, StandardCharsets.UTF_8));

		this.transport = new StdioMcpTransport(manager, this.jsonMapper, this.serverIn, this.serverOut);
		this.transport.start().subscribe();
	}

	@AfterEach
	void tearDown() {
		if (this.transport != null) {
			this.transport.closeGracefully().block();
		}
	}

	private void send(String method, Object id, Map<String, Object> params) throws IOException {
		Map<String, Object> body = new java.util.HashMap<>();
		body.put("jsonrpc", "2.0");
		body.put("method", method);
		if (id != null) {
			body.put("id", id);
		}
		body.put("params", params);
		this.clientOut.write((this.jsonMapper.writeValueAsString(body) + "\n").getBytes(StandardCharsets.UTF_8));
		this.clientOut.flush();
	}

	private static McpRequestManager managerOf(
			java.util.function.BiFunction<McpTransportContext, JSONRPCRequest, Mono<McpInvocation>> resolveFn) {
		return new McpRequestManager() {
			@Override
			public Mono<McpInvocation> resolveBlocking(McpTransportContext transportContext, JSONRPCRequest request) {
				throw new AssertionError("stdio must never resolve for a blocking caller");
			}

			@Override
			public Mono<McpInvocation> resolveNonBlocking(McpTransportContext transportContext,
					JSONRPCRequest request) {
				return resolveFn.apply(transportContext, request);
			}

			@Override
			public Mono<Void> handleNotification(McpTransportContext transportContext,
					JSONRPCNotification notification) {
				return Mono.empty();
			}
		};
	}

	private static Map<String, Object> meta(Object... extra) {
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		for (int i = 0; i < extra.length; i += 2) {
			meta.put((String) extra[i], extra[i + 1]);
		}
		return meta;
	}

	@Test
	void fastRequestIsNotBlockedByASlowerConcurrentOne() throws Exception {
		CountDownLatch slowStarted = new CountDownLatch(1);
		CountDownLatch releaseSlow = new CountDownLatch(1);

		McpRequestManager manager = managerOf((transportContext, request) -> {
			boolean slow = "slow".equals(((Map<?, ?>) request.params()).get("name"));
			Mono<JSONRPCResponse> response = Mono.fromCallable(() -> {
				if (slow) {
					slowStarted.countDown();
					releaseSlow.await(5, TimeUnit.SECONDS);
				}
				return JSONRPCResponse.result(request.id(),
						Map.of("resultType", "complete", "content", java.util.List.of()));
			});
			return Mono
				.just(McpInvocation.single(response.subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())));
		});

		start(manager);

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("name", "slow");
		send("tools/call", 1, params);
		assertThat(slowStarted.await(5, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> fastParams = new java.util.HashMap<>();
		fastParams.put("_meta", meta());
		fastParams.put("name", "fast");
		send("tools/call", 2, fastParams);

		String firstLine = readLineWithTimeout();
		Map<String, Object> firstResponse = this.jsonMapper.readValue(firstLine, Map.class);
		assertThat(((Number) firstResponse.get("id")).intValue()).isEqualTo(2);

		releaseSlow.countDown();
		String secondLine = readLineWithTimeout();
		Map<String, Object> secondResponse = this.jsonMapper.readValue(secondLine, Map.class);
		assertThat(((Number) secondResponse.get("id")).intValue()).isEqualTo(1);
	}

	@Test
	void streamingRequestWritesNotificationsBeforeResponse() throws Exception {
		McpRequestManager manager = managerOf((transportContext,
				request) -> Mono.just(McpInvocation.streaming(reactor.core.publisher.Flux.just(
						(JSONRPCMessage) new JSONRPCNotification("notifications/progress", Map.of("progress", 1.0)),
						JSONRPCResponse.result(request.id(), Map.of("resultType", "complete"))))));

		start(manager);
		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("name", "streamer");
		send("tools/call", 5, params);

		String first = readLineWithTimeout();
		assertThat(first).contains("notifications/progress");
		String second = readLineWithTimeout();
		assertThat(second).contains("\"result\"");
	}

	@Test
	void cancelledNotificationStopsOutputForThatRequest() throws Exception {
		AtomicReference<Boolean> sawCancel = new AtomicReference<>(false);
		McpRequestManager manager = managerOf((transportContext, request) -> {
			Mono<JSONRPCResponse> response = Mono.<JSONRPCResponse>never().doOnCancel(() -> sawCancel.set(true));
			return Mono.just(McpInvocation.single(response));
		});
		start(manager);

		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("name", "hangs");
		send("tools/call", 9, params);

		Thread.sleep(300); // let dispatch register before the cancel arrives
		send("notifications/cancelled", null, Map.of("requestId", 9));

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(sawCancel.get()).isTrue());
	}

	@Test
	void invalidJsonProducesParseError() throws Exception {
		McpRequestManager manager = managerOf((transportContext, request) -> Mono
			.just(McpInvocation.single(Mono.just(JSONRPCResponse.result(request.id(), Map.of())))));
		start(manager);

		this.clientOut.write("not json at all\n".getBytes(StandardCharsets.UTF_8));
		this.clientOut.flush();

		String line = readLineWithTimeout();
		Map<String, Object> parsed = this.jsonMapper.readValue(line, Map.class);
		assertThat(parsed.get("error")).isNotNull();
	}

	@Test
	void closeGracefullyCompletesTheStartMono() throws Exception {
		McpRequestManager manager = managerOf((transportContext, request) -> Mono
			.just(McpInvocation.single(Mono.just(JSONRPCResponse.result(request.id(), Map.of())))));
		this.clientOut = new PipedOutputStream();
		this.serverIn = new PipedInputStream(this.clientOut);
		PipedInputStream clientIn = new PipedInputStream();
		this.serverOut = new PipedOutputStream(clientIn);
		this.transport = new StdioMcpTransport(manager, this.jsonMapper, this.serverIn, this.serverOut);

		CountDownLatch done = new CountDownLatch(1);
		this.transport.start().doFinally(s -> done.countDown()).subscribe();

		this.transport.closeGracefully().block();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
	}

	private String readLineWithTimeout() throws IOException {
		// BufferedReader#readLine blocks until data or EOF; run it on a separate
		// thread so a design bug (no output ever written) fails with a timeout
		// instead of hanging the test forever.
		java.util.concurrent.CompletableFuture<String> future = java.util.concurrent.CompletableFuture
			.supplyAsync(() -> {
				try {
					return this.serverResponses.readLine();
				}
				catch (IOException e) {
					throw new java.util.concurrent.CompletionException(e);
				}
			});
		try {
			return future.get(5, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			throw new IOException("Timed out waiting for a line", e);
		}
	}

}
