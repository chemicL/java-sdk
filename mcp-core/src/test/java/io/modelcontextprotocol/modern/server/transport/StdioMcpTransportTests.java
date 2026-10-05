/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.transport;

import java.io.BufferedReader;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.server.McpTransportResponse;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.modern.server.feature.McpChangeFeed;
import io.modelcontextprotocol.modern.server.feature.ToolsFeature;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static io.modelcontextprotocol.modern.server.ModernTestFixtures.SERVER_INFO;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.emptyTools;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.meta;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class StdioMcpTransportTests {

	private final GsonMcpJsonMapper jsonMapper = new GsonMcpJsonMapper();

	private PipedOutputStream clientOut;

	private PipedInputStream serverIn;

	private PipedOutputStream serverOut;

	private BufferedReader serverResponses;

	private StdioMcpTransport transport;

	private Mono<Void> served;

	private void start(McpRequestManager manager) throws IOException {
		start(manager, this.jsonMapper, UnaryOperator.identity());
	}

	private void start(McpRequestManager manager, McpJsonMapper serverMapper, UnaryOperator<OutputStream> wrapOut)
			throws IOException {
		this.clientOut = new PipedOutputStream();
		this.serverIn = new PipedInputStream(this.clientOut);
		PipedInputStream clientIn = new PipedInputStream();
		this.serverOut = new PipedOutputStream(clientIn);
		this.serverResponses = new BufferedReader(new InputStreamReader(clientIn, StandardCharsets.UTF_8));

		this.transport = new StdioMcpTransport(manager, serverMapper, this.serverIn, wrapOut.apply(this.serverOut));
		this.served = this.transport.start();
		this.served.subscribe();
	}

	private void sendRaw(String line) throws IOException {
		this.clientOut.write((line + "\n").getBytes(StandardCharsets.UTF_8));
		this.clientOut.flush();
	}

	private static int errorCode(Map<String, Object> response) {
		return ((Number) ((Map<?, ?>) response.get("error")).get("code")).intValue();
	}

	@AfterEach
	void tearDown() {
		if (this.transport != null) {
			this.transport.closeGracefully().block();
		}
	}

	private void send(String method, Object id, Map<String, Object> params) throws IOException {
		Map<String, Object> body = new HashMap<>();
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
			BiFunction<McpTransportContext, JSONRPCRequest, Mono<McpTransportResponse>> handleFn) {
		return new McpRequestManager() {
			@Override
			public Mono<McpTransportResponse> handleBlocking(McpTransportContext transportContext,
					JSONRPCRequest request) {
				throw new AssertionError("stdio must never handle requests for a blocking caller");
			}

			@Override
			public Mono<McpTransportResponse> handle(McpTransportContext transportContext, JSONRPCRequest request) {
				return handleFn.apply(transportContext, request);
			}

			@Override
			public Mono<Void> handleNotification(McpTransportContext transportContext,
					JSONRPCNotification notification) {
				return Mono.empty();
			}
		};
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
				return JSONRPCResponse.result(request.id(), Map.of("resultType", "complete", "content", List.of()));
			});
			return response.subscribeOn(Schedulers.boundedElastic()).map(McpTransportResponse::result);
		});

		start(manager);

		Map<String, Object> params = new HashMap<>();
		params.put("_meta", meta());
		params.put("name", "slow");
		send("tools/call", 1, params);
		assertThat(slowStarted.await(5, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> fastParams = new HashMap<>();
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
				request) -> Mono.just(McpTransportResponse.streaming(Flux.just(
						(JSONRPCMessage) new JSONRPCNotification("notifications/progress", Map.of("progress", 1.0)),
						JSONRPCResponse.result(request.id(), Map.of("resultType", "complete"))))));

		start(manager);
		Map<String, Object> params = new HashMap<>();
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
			return Mono.<McpTransportResponse>never().doOnCancel(() -> sawCancel.set(true));
		});
		start(manager);

		Map<String, Object> params = new HashMap<>();
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
			.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of()))));
		start(manager);

		this.clientOut.write("not json at all\n".getBytes(StandardCharsets.UTF_8));
		this.clientOut.flush();

		String line = readLineWithTimeout();
		Map<String, Object> parsed = this.jsonMapper.readValue(line, Map.class);
		assertThat(parsed.get("id")).isNull();
		assertThat(((Number) ((Map<?, ?>) parsed.get("error")).get("code")).intValue())
			.isEqualTo(ErrorCodes.PARSE_ERROR);
	}

	@Test
	void invalidEnvelopeProducesInvalidRequestAndTransportKeepsServing() throws Exception {
		McpRequestManager manager = managerOf((transportContext, request) -> Mono
			.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of()))));
		start(manager);

		this.clientOut.write("{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"tools/list\",\"params\":{}}\n"
			.getBytes(StandardCharsets.UTF_8));
		this.clientOut.flush();

		Map<String, Object> parsed = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(parsed.get("id")).isNull();
		assertThat(((Number) ((Map<?, ?>) parsed.get("error")).get("code")).intValue())
			.isEqualTo(ErrorCodes.INVALID_REQUEST);

		send("tools/list", 7, Map.of("_meta", meta()));
		Map<String, Object> next = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) next.get("id")).intValue()).isEqualTo(7);
	}

	@Test
	void concurrentResponsesAreWrittenWholeAndNoneIsLost() throws Exception {
		McpRequestManager manager = managerOf(
				(transportContext, request) -> Mono.fromCallable(() -> JSONRPCResponse.result(request.id(), Map.of()))
					.subscribeOn(Schedulers.parallel())
					.map(McpTransportResponse::result));
		start(manager);

		int count = 200;
		for (int i = 0; i < count; i++) {
			send("tools/list", i, Map.of("_meta", meta()));
		}

		Set<Integer> ids = new HashSet<>();
		for (int i = 0; i < count; i++) {
			Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
			ids.add(((Number) response.get("id")).intValue());
		}
		assertThat(ids).hasSize(count);
	}

	@Test
	void duplicateInFlightRequestIdIsIgnored() throws Exception {
		McpRequestManager manager = managerOf(
				(transportContext, request) -> "hangs".equals(((Map<?, ?>) request.params()).get("name")) ? Mono.never()
						: Mono.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of()))));
		start(manager);

		send("tools/call", 1, Map.of("_meta", meta(), "name", "hangs"));
		send("tools/call", 1, Map.of("_meta", meta(), "name", "answers"));
		send("tools/call", 2, Map.of("_meta", meta(), "name", "answers"));

		// An error for id 1 would read as the answer to the request still in flight.
		Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) response.get("id")).intValue()).isEqualTo(2);
		assertThat(response.get("error")).isNull();
	}

	@Test
	void nonObjectJsonProducesInvalidRequest() throws Exception {
		start(managerOf((transportContext, request) -> Mono.never()));

		for (String line : List.of("null", "[]", "42", "\"text\"")) {
			sendRaw(line);
			Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
			assertThat(response.get("id")).as(line).isNull();
			assertThat(errorCode(response)).as(line).isEqualTo(ErrorCodes.INVALID_REQUEST);
		}
	}

	@Test
	void invalidRequestIsAnsweredWithItsId() throws Exception {
		start(managerOf((transportContext, request) -> Mono.never()));

		sendRaw("{\"jsonrpc\":\"1.0\",\"id\":8,\"method\":\"tools/list\",\"params\":{}}");

		Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) response.get("id")).intValue()).isEqualTo(8);
		assertThat(errorCode(response)).isEqualTo(ErrorCodes.INVALID_REQUEST);
	}

	@Test
	void cancellationDropsOutputQueuedBeforeIt() throws Exception {
		CountDownLatch firstWriteStarted = new CountDownLatch(1);
		CountDownLatch releaseWrites = new CountDownLatch(1);
		CountDownLatch notificationQueued = new CountDownLatch(1);
		AtomicReference<Boolean> sawCancel = new AtomicReference<>(false);
		McpRequestManager manager = managerOf((transportContext, request) -> {
			if (!"streams".equals(((Map<?, ?>) request.params()).get("name"))) {
				return Mono.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of())));
			}
			// Concat moves on only once the notification's onNext has returned, so the
			// latch opens after the transport has queued it.
			Flux<JSONRPCMessage> messages = Flux
				.concat(Flux.just((JSONRPCMessage) new JSONRPCNotification("notifications/progress", Map.of())),
						Mono.fromRunnable(notificationQueued::countDown).then(Mono.<JSONRPCMessage>never()))
				.doOnCancel(() -> sawCancel.set(true));
			return Mono.just(McpTransportResponse.streaming(messages));
		});
		// Holds the writer on its first message, so later output stays queued.
		start(manager, this.jsonMapper, stream -> new FilterOutputStream(stream) {
			@Override
			public void write(byte[] b) throws IOException {
				firstWriteStarted.countDown();
				try {
					releaseWrites.await(5, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				out.write(b);
			}
		});

		send("tools/call", 1, Map.of("_meta", meta(), "name", "answers"));
		assertThat(firstWriteStarted.await(5, TimeUnit.SECONDS)).isTrue();
		send("tools/call", 2, Map.of("_meta", meta(), "name", "streams"));
		assertThat(notificationQueued.await(5, TimeUnit.SECONDS)).isTrue();
		send("notifications/cancelled", null, Map.of("requestId", 2));
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(sawCancel.get()).isTrue());
		send("tools/call", 3, Map.of("_meta", meta(), "name", "answers"));
		releaseWrites.countDown();

		Map<String, Object> first = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) first.get("id")).intValue()).isEqualTo(1);
		Map<String, Object> next = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(next.get("method")).isNull();
		assertThat(((Number) next.get("id")).intValue()).isEqualTo(3);
	}

	@Test
	void asyncHandlerThatBlocksDoesNotStallReading() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		McpRequestManager manager = managerOf((transportContext, request) -> {
			boolean slow = "slow".equals(((Map<?, ?>) request.params()).get("name"));
			// No subscribeOn: this blocks whichever thread subscribes.
			return Mono.fromCallable(() -> {
				if (slow) {
					release.await(5, TimeUnit.SECONDS);
				}
				return McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of()));
			});
		});
		start(manager);

		send("tools/call", 1, Map.of("_meta", meta(), "name", "slow"));
		send("tools/call", 2, Map.of("_meta", meta(), "name", "fast"));

		Map<String, Object> first = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) first.get("id")).intValue()).isEqualTo(2);
		release.countDown();
		Map<String, Object> second = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) second.get("id")).intValue()).isEqualTo(1);
	}

	@Test
	void prettyPrintedOutputStaysOnOneLine() throws Exception {
		GsonMcpJsonMapper prettyMapper = new GsonMcpJsonMapper(new GsonBuilder().setPrettyPrinting()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.create());
		start(managerOf((transportContext,
				request) -> Mono.just(McpTransportResponse
					.result(JSONRPCResponse.result(request.id(), Map.of("text", "line one\nline two"))))),
				prettyMapper, UnaryOperator.identity());

		send("tools/call", 6, Map.of("_meta", meta()));

		Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) response.get("id")).intValue()).isEqualTo(6);
		assertThat(((Map<?, ?>) response.get("result")).get("text")).isEqualTo("line one\nline two");
	}

	@Test
	void closeFinishesEvenIfTheCallerStopsWaiting() throws Exception {
		CountDownLatch listening = new CountDownLatch(1);
		start(managerOf((transportContext, request) -> {
			listening.countDown();
			return Mono.never();
		}));
		CountDownLatch done = new CountDownLatch(1);
		this.served.doFinally(s -> done.countDown()).subscribe();

		send("subscriptions/listen", 1, Map.of("_meta", meta()));
		assertThat(listening.await(5, TimeUnit.SECONDS)).isTrue();

		// The never-ending request holds the grace period longer than the caller waits.
		assertThatThrownBy(() -> this.transport.closeGracefully().block(Duration.ofMillis(100)))
			.isInstanceOf(IllegalStateException.class);
		assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	void startCanOnlyBeCalledOnce() throws Exception {
		start(managerOf((transportContext, request) -> Mono.never()));

		assertThatThrownBy(() -> this.transport.start()).isInstanceOf(IllegalStateException.class);
	}

	@Test
	void idOfAnsweredRequestCanBeReused() throws Exception {
		McpRequestManager manager = managerOf(
				(transportContext, request) -> Mono.fromCallable(() -> JSONRPCResponse.result(request.id(), Map.of()))
					.subscribeOn(Schedulers.parallel())
					.map(McpTransportResponse::result));
		start(manager);

		for (int i = 0; i < 50; i++) {
			send("tools/list", 1, Map.of("_meta", meta()));
			Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
			assertThat(response.get("error")).isNull();
		}
	}

	@Test
	void eofAnswersInFlightRequestsBeforeCompleting() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		McpRequestManager manager = managerOf((transportContext, request) -> Mono.fromCallable(() -> {
			release.await(5, TimeUnit.SECONDS);
			return JSONRPCResponse.result(request.id(), Map.of());
		}).subscribeOn(Schedulers.boundedElastic()).map(McpTransportResponse::result));
		start(manager);

		send("tools/list", 4, Map.of("_meta", meta()));
		Thread.sleep(300); // let dispatch register before stdin closes
		this.clientOut.close();
		release.countDown();

		Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) response.get("id")).intValue()).isEqualTo(4);
		// Already closing after EOF, so this only awaits completion.
		this.transport.closeGracefully().block(Duration.ofSeconds(5));
	}

	@Test
	void closeCompletesOnAThreadThatAllowsBlocking() throws Exception {
		start(managerOf((transportContext, request) -> Mono.empty()));

		this.transport.closeGracefully()
			.then(Mono.fromRunnable(() -> Mono.delay(Duration.ofMillis(1)).block()))
			.block(Duration.ofSeconds(5));
	}

	@Test
	void requestDuringGracefulCloseIsRejected() throws Exception {
		CountDownLatch listening = new CountDownLatch(1);
		McpRequestManager manager = managerOf((transportContext, request) -> {
			if ("subscriptions/listen".equals(request.method())) {
				listening.countDown();
				return Mono.never();
			}
			return Mono.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of())));
		});
		start(manager);

		send("subscriptions/listen", 1, Map.of("_meta", meta()));
		assertThat(listening.await(5, TimeUnit.SECONDS)).isTrue();

		// The never-ending listen stream holds the transport in its grace period.
		this.transport.closeGracefully().subscribe();
		send("tools/list", 2, Map.of("_meta", meta()));

		Map<String, Object> response = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) response.get("id")).intValue()).isEqualTo(2);
		assertThat(((Number) ((Map<?, ?>) response.get("error")).get("code")).intValue())
			.isEqualTo(ErrorCodes.INTERNAL_ERROR);
	}

	@Test
	void closeGracefullyEndsListenStreamsWithCompleteResult() throws Exception {
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(this.jsonMapper)
			.feature(ToolsFeature.ofAsync(emptyTools(), this.jsonMapper, 0L, CacheScope.PRIVATE))
			.subscriptions(McpChangeFeed.sink())
			.build();
		start(server);

		send("subscriptions/listen", 3, Map.of("_meta", meta(), "notifications", Map.of("toolsListChanged", true)));
		assertThat(readLineWithTimeout()).contains("notifications/subscriptions/acknowledged");

		this.transport.closeGracefully().block(Duration.ofSeconds(5));

		Map<String, Object> closing = this.jsonMapper.readValue(readLineWithTimeout(), Map.class);
		assertThat(((Number) closing.get("id")).intValue()).isEqualTo(3);
		assertThat(closing.get("result")).isNotNull();
	}

	@Test
	void closeGracefullyCompletesTheStartMono() throws Exception {
		McpRequestManager manager = managerOf((transportContext, request) -> Mono
			.just(McpTransportResponse.result(JSONRPCResponse.result(request.id(), Map.of()))));
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
		CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
			try {
				return this.serverResponses.readLine();
			}
			catch (IOException e) {
				throw new CompletionException(e);
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
