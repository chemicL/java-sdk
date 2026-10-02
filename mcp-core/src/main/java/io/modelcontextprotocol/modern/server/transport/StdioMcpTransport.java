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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.JsonRpc;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A newline-delimited stdio transport for a modern {@link McpRequestManager}. There is no
 * session and no framing beyond one JSON-RPC message per line.
 * <p>
 * A single dedicated thread reads stdin and dispatches; it never blocks on a request's
 * own completion, so a slow request cannot delay a faster concurrent one, and a
 * {@code notifications/cancelled} for one request is always readable while others are in
 * flight: requests are resolved with {@link McpRequestManager#resolveNonBlocking}, so
 * sync repositories and handlers never run on the reader thread. Writes are serialized
 * through one sink onto one dedicated writer thread.
 *
 * @author Dariusz Jędrzejczyk
 */
public class StdioMcpTransport {

	private static final Logger logger = LoggerFactory.getLogger(StdioMcpTransport.class);

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

	private final Map<String, Disposable> inFlight = new ConcurrentHashMap<>();

	private final Sinks.Empty<Void> completion = Sinks.empty();

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
		this.writerSink.asFlux().publishOn(this.writerScheduler).doOnNext(this::writeLine).subscribe();
		this.readerScheduler.schedule(this::readLoop);
		return this.completion.asMono();
	}

	public Mono<Void> closeGracefully() {
		return Mono.fromRunnable(this::shutdown);
	}

	private void shutdown() {
		this.closing = true;
		this.inFlight.values().forEach(Disposable::dispose);
		this.inFlight.clear();
		synchronized (this.writerLock) {
			this.writerSink.tryEmitComplete();
		}
		this.completion.tryEmitEmpty();
		this.readerScheduler.dispose();
		this.writerScheduler.dispose();
	}

	private void readLoop() {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.in, StandardCharsets.UTF_8))) {
			String line;
			while (!this.closing && (line = reader.readLine()) != null) {
				if (!line.isBlank()) {
					handleLine(line);
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
		catch (IllegalArgumentException | IOException e) {
			emitParseError();
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
		Disposable subscription = this.inFlight.remove(String.valueOf(requestId));
		if (subscription != null) {
			subscription.dispose();
		}
	}

	private void dispatch(JSONRPCRequest request) {
		String key = String.valueOf(request.id());
		Flux<JSONRPCMessage> flux = this.requestManager.resolveNonBlocking(McpTransportContext.EMPTY, request)
			.flatMapMany(invocation -> {
				if (invocation instanceof McpInvocation.Streaming streaming) {
					return streaming.messages();
				}
				return ((McpInvocation.Single) invocation).response().flux();
			});

		Disposable subscription = flux.doFinally(signal -> this.inFlight.remove(key))
			.subscribe(this::emit, err -> logger.warn("Unhandled error dispatching request {}", key, err));

		this.inFlight.put(key, subscription);
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

	private void emitParseError() {
		Map<String, Object> error = new LinkedHashMap<>();
		error.put("code", McpSchema.ErrorCodes.PARSE_ERROR);
		error.put("message", "Invalid message format");
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("jsonrpc", "2.0");
		body.put("id", null);
		body.put("error", error);
		try {
			String json = this.jsonMapper.writeValueAsString(body);
			this.writerScheduler.schedule(() -> writeRaw(json));
		}
		catch (IOException e) {
			logger.warn("Failed to serialize parse error", e);
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

}
