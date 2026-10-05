/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.JsonRpc;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.modern.server.McpTransportResponse;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A newline-delimited stdio transport for a modern {@link McpRequestManager}: one
 * JSON-RPC message per line, no session. Requests are resolved non-blocking, so a slow
 * request does not delay others.
 *
 * @author Dariusz Jędrzejczyk
 */
public class StdioMcpTransport {

	private static final Logger logger = LoggerFactory.getLogger(StdioMcpTransport.class);

	private static final Duration GRACE_PERIOD = Duration.ofSeconds(2);

	private final McpRequestManager requestManager;

	private final McpJsonMapper jsonMapper;

	private final InputStream in;

	private final OutputStream out;

	private final Scheduler readerScheduler = Schedulers.newSingle("mcp-stdio-reader");

	private final Scheduler writerScheduler = Schedulers.newSingle("mcp-stdio-writer");

	// unicast() + onBackpressureBuffer() is not itself safe for concurrent producers;
	// emit() below synchronizes so only one thread ever calls tryEmitNext at a time.
	private final Sinks.Many<JSONRPCMessage> writerSink = Sinks.many().unicast().onBackpressureBuffer();

	private final Object writerLock = new Object();

	private final Map<Object, InFlight> inFlight = new ConcurrentHashMap<>();

	private final Sinks.Empty<Void> writerDone = Sinks.empty();

	private final Sinks.Empty<Void> completion = Sinks.empty();

	private final AtomicBoolean shutDown = new AtomicBoolean();

	private volatile boolean closing = false;

	public StdioMcpTransport(McpRequestManager requestManager, McpJsonMapper jsonMapper) {
		this(requestManager, jsonMapper, System.in, System.out);
	}

	public StdioMcpTransport(McpRequestManager requestManager, McpJsonMapper jsonMapper, InputStream in,
			OutputStream out) {
		Assert.notNull(requestManager, "requestManager must not be null");
		Assert.notNull(jsonMapper, "jsonMapper must not be null");
		this.requestManager = requestManager;
		this.jsonMapper = jsonMapper;
		this.in = in;
		this.out = out;
	}

	/**
	 * Start reading. Completes when stdin reaches EOF or {@link #closeGracefully()} is
	 * called.
	 */
	public Mono<Void> start() {
		this.writerSink.asFlux()
			.publishOn(this.writerScheduler)
			.doOnNext(this::writeLine)
			.doFinally(signal -> this.writerDone.tryEmitEmpty())
			.subscribe();
		this.readerScheduler.schedule(this::readLoop);
		return this.completion.asMono();
	}

	/**
	 * Stop reading and end all requests. If the request manager is a {@link McpServer},
	 * active {@code subscriptions/listen} streams first end with their graceful
	 * {@code complete} result.
	 */
	public Mono<Void> closeGracefully() {
		return Mono.defer(() -> {
			if (this.closing) {
				return this.completion.asMono();
			}
			this.closing = true;
			if (this.requestManager instanceof McpServer server) {
				server.closeGracefully();
			}
			List<Mono<Void>> listenStreams = this.inFlight.values()
				.stream()
				.filter(InFlight::listen)
				.map(InFlight::done)
				.toList();
			return Mono.when(listenStreams)
				.timeout(GRACE_PERIOD, Mono.empty())
				.then(Mono.fromRunnable(this::shutdown))
				.then(this.completion.asMono());
		});
	}

	private void shutdown() {
		if (!this.shutDown.compareAndSet(false, true)) {
			return;
		}
		this.closing = true;
		this.inFlight.values().forEach(entry -> entry.subscription().dispose());
		this.inFlight.clear();
		synchronized (this.writerLock) {
			this.writerSink.tryEmitComplete();
		}
		// Let already-queued output reach stdout before the writer thread goes away.
		this.writerDone.asMono().timeout(GRACE_PERIOD, Mono.empty()).onErrorComplete().doFinally(signal -> {
			this.completion.tryEmitEmpty();
			this.readerScheduler.dispose();
			this.writerScheduler.dispose();
		}).subscribe();
	}

	private void readLoop() {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.in, StandardCharsets.UTF_8))) {
			String line;
			while (!this.closing && (line = reader.readLine()) != null) {
				if (line.isBlank()) {
					continue;
				}
				// One bad message must never end the read loop - it would take the whole
				// server down.
				try {
					handleLine(line);
				}
				catch (RuntimeException ex) {
					logger.warn("Failed to handle stdio message", ex);
				}
			}
		}
		catch (IOException e) {
			logger.warn("stdio read failed", e);
		}
		finally {
			shutdown();
		}
	}

	private void handleLine(String line) {
		JSONRPCMessage message;
		try {
			message = JsonRpc.deserializeMessage(this.jsonMapper, line);
		}
		catch (IOException e) {
			emit(JSONRPCResponse.error(null, new JSONRPCError(McpSchema.ErrorCodes.PARSE_ERROR, "Parse error")));
			return;
		}
		catch (IllegalArgumentException e) {
			emit(JSONRPCResponse.error(null,
					new JSONRPCError(McpSchema.ErrorCodes.INVALID_REQUEST, "Invalid JSON-RPC message")));
			return;
		}

		if (message instanceof JSONRPCNotification notification) {
			if (McpSchema.METHOD_NOTIFICATION_CANCELLED.equals(notification.method())) {
				handleCancel(notification);
			}
			else {
				this.requestManager.handleNotification(McpTransportContext.EMPTY, notification).subscribe(v -> {
				}, err -> logger.warn("Failed to handle notification", err));
			}
			return;
		}

		if (message instanceof JSONRPCRequest request) {
			dispatch(request);
		}
		// Modern servers never expect a JSON-RPC response from a client; ignore.
	}

	private void handleCancel(JSONRPCNotification notification) {
		Object requestId = null;
		if (notification.params() instanceof Map<?, ?> params) {
			requestId = params.get("requestId");
		}
		if (requestId == null) {
			return;
		}
		InFlight entry = this.inFlight.remove(keyOf(requestId));
		if (entry != null) {
			entry.subscription().dispose();
		}
	}

	private void dispatch(JSONRPCRequest request) {
		Object key = keyOf(request.id());
		Flux<JSONRPCMessage> flux = this.requestManager.handle(McpTransportContext.EMPTY, request)
			.flatMapMany(StdioMcpTransport::messages);

		// Registered before subscribing: a request that completes synchronously removes
		// its own entry in doFinally, which must not run before the put.
		Sinks.Empty<Void> done = Sinks.empty();
		InFlight entry = new InFlight(Disposables.swap(), done.asMono(),
				McpSchema.METHOD_SUBSCRIPTIONS_LISTEN.equals(request.method()));
		this.inFlight.put(key, entry);
		entry.subscription().update(flux.doFinally(signal -> {
			this.inFlight.remove(key, entry);
			done.tryEmitEmpty();
		}).subscribe(this::emit, err -> logger.warn("Unhandled error dispatching request {}", key, err)));
	}

	// stdio has no status channel: every response is just its messages.
	private static Flux<JSONRPCMessage> messages(McpTransportResponse response) {
		if (response instanceof McpTransportResponse.Streaming streaming) {
			return streaming.messages();
		}
		if (response instanceof McpTransportResponse.Result result) {
			return Flux.just(result.response());
		}
		return Flux.just(((McpTransportResponse.Error) response).response());
	}

	// Ids 1 and 1L must find the same entry, while "1" stays distinct.
	private static Object keyOf(Object id) {
		return id instanceof Number number ? (Object) number.longValue() : id;
	}

	private void emit(JSONRPCMessage message) {
		Sinks.EmitResult result;
		synchronized (this.writerLock) {
			result = this.writerSink.tryEmitNext(message);
		}
		if (result.isFailure()) {
			logger.warn("Failed to enqueue outbound message: {}", result);
		}
	}

	private void writeLine(JSONRPCMessage message) {
		try {
			writeRaw(this.jsonMapper.writeValueAsString(message));
		}
		catch (IOException e) {
			logger.warn("Failed to serialize outbound message", e);
		}
	}

	private void writeRaw(String json) {
		try {
			this.out.write(json.getBytes(StandardCharsets.UTF_8));
			this.out.write('\n');
			this.out.flush();
		}
		catch (IOException e) {
			logger.warn("stdio write failed", e);
		}
	}

	private record InFlight(Disposable.Swap subscription, Mono<Void> done, boolean listen) {
	}

}
