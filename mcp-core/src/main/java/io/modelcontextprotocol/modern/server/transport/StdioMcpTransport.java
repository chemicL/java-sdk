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
import java.util.concurrent.RejectedExecutionException;
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
 * JSON-RPC message per line, no session. Requests are handled off the reading thread, so
 * a slow request does not delay others. Nothing else may write to the output stream; with
 * the default constructor, route all logging to stderr.
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

	// Daemon threads: a read blocked on System.in cannot be interrupted, so the reader
	// could outlive close. The transport's lifetime is start()'s Mono instead.
	private final Scheduler readerScheduler = Schedulers.newSingle("mcp-stdio-reader", true);

	private final Scheduler writerScheduler = Schedulers.newSingle("mcp-stdio-writer", true);

	// A worker runs its tasks one at a time in FIFO order and accepts them from any
	// thread, which is all the serialization concurrent responses need.
	private final Scheduler.Worker writer = this.writerScheduler.createWorker();

	private final Map<Object, InFlight> inFlight = new ConcurrentHashMap<>();

	private final Sinks.Empty<Void> completion = Sinks.empty();

	private final AtomicBoolean started = new AtomicBoolean();

	private final AtomicBoolean closing = new AtomicBoolean();

	private final AtomicBoolean shutDown = new AtomicBoolean();

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
	 * Start reading; may be called once. Completes when stdin reaches EOF or
	 * {@link #closeGracefully()} is called. The transport's threads do not keep the JVM
	 * alive, so block on the returned Mono to serve until then.
	 * @throws IllegalStateException if already started
	 */
	public Mono<Void> start() {
		if (!this.started.compareAndSet(false, true)) {
			throw new IllegalStateException("StdioMcpTransport can only be started once");
		}
		try {
			this.readerScheduler.schedule(this::readLoop);
		}
		catch (RejectedExecutionException e) {
			// Closed before it was started: there is nothing to read.
		}
		return this.completion.asMono();
	}

	/**
	 * Stop accepting requests and give in-flight ones a short grace period to finish
	 * before ending them. If the request manager is a {@link McpServer}, active
	 * {@code subscriptions/listen} streams end with their graceful {@code complete}
	 * result. Completes once the transport has shut down; cancelling the returned Mono
	 * does not stop the shutdown.
	 */
	public Mono<Void> closeGracefully() {
		return Mono.defer(() -> {
			if (this.closing.compareAndSet(false, true)) {
				beginClose();
			}
			return this.completion.asMono();
		});
	}

	private void beginClose() {
		if (this.requestManager instanceof McpServer server) {
			server.closeGracefully();
		}
		List<Mono<Void>> pending = this.inFlight.values().stream().map(InFlight::done).toList();
		// Subscribed here rather than returned: a caller that stops waiting, e.g. with
		// block(timeout), would otherwise cancel the shutdown and leave the transport
		// closing forever.
		Mono.when(pending).timeout(GRACE_PERIOD, Mono.empty()).doFinally(signal -> shutdown()).subscribe();
	}

	private void shutdown() {
		if (!this.shutDown.compareAndSet(false, true)) {
			return;
		}
		this.closing.set(true);
		this.inFlight.values().forEach(entry -> entry.subscription().dispose());
		this.inFlight.clear();
		// Queued behind all pending output, so it signals once that output is written.
		Sinks.Empty<Void> drained = Sinks.empty();
		this.writer.schedule(drained::tryEmitEmpty);
		drained.asMono()
			.timeout(GRACE_PERIOD, Mono.empty())
			// Off the writer thread: it is disposed here, and completion subscribers
			// must not run on a thread that rejects blocking.
			.publishOn(Schedulers.boundedElastic())
			.doFinally(signal -> {
				this.writer.dispose();
				this.readerScheduler.dispose();
				this.writerScheduler.dispose();
				this.completion.tryEmitEmpty();
			})
			.subscribe();
	}

	private void readLoop() {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.in, StandardCharsets.UTF_8))) {
			String line;
			// Checked after each read: a line that arrives once shut down is dropped.
			while ((line = reader.readLine()) != null && !this.shutDown.get()) {
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
			// Disposing the reader on shutdown interrupts a read on interruptible
			// streams.
			if (!this.shutDown.get()) {
				logger.warn("stdio read failed", e);
			}
		}
		finally {
			// The client may close stdin right after its last request and still read
			// the responses, so EOF closes gracefully too.
			closeGracefully().subscribe();
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
		catch (JsonRpc.InvalidMessageException e) {
			emit(JSONRPCResponse.error(e.id(),
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
		if (!(notification.params() instanceof Map<?, ?> params) || params.get("requestId") == null) {
			return;
		}
		Object requestId = params.get("requestId");
		InFlight entry = this.inFlight.remove(keyOf(requestId));
		if (entry != null) {
			logger.debug("Request {} cancelled by the client: {}", requestId, params.get("reason"));
			entry.cancelled().set(true);
			entry.subscription().dispose();
		}
	}

	private void dispatch(JSONRPCRequest request) {
		Object key = keyOf(request.id());
		// Registered before subscribing: a request that completes synchronously removes
		// its own entry in doFinally, which must not run before the put.
		Sinks.Empty<Void> done = Sinks.empty();
		InFlight entry = new InFlight(Disposables.swap(), done.asMono(), new AtomicBoolean());
		if (this.inFlight.putIfAbsent(key, entry) != null) {
			// Not answered: an error carrying this id would be taken by the client as the
			// response to the request still in flight.
			logger.warn("Ignoring request {}: a request with this id is still in flight", request.id());
			return;
		}
		// Checked after registering: closing sets the flag before it looks at inFlight,
		// so either it sees this entry or this sees the flag.
		if (this.closing.get()) {
			this.inFlight.remove(key, entry);
			emit(JSONRPCResponse.error(request.id(),
					new JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR, "Server is shutting down")));
			return;
		}

		Flux<JSONRPCMessage> flux = this.requestManager.handle(McpTransportContext.EMPTY, request)
			// Async handlers run on the subscribing thread; keep them off the reader so
			// it can always read the next request or cancellation.
			.subscribeOn(Schedulers.boundedElastic())
			.flatMapMany(StdioMcpTransport::messages)
			// Cancellation and shutdown dispose the subscription while an error may be
			// on its way. A cancelled subscriber drops errors before any error consumer
			// runs; onErrorComplete absorbs them even after cancellation.
			.doOnError(err -> logger.warn("Unhandled error dispatching request {}", key, err))
			.onErrorComplete();
		entry.subscription().update(flux.doFinally(signal -> {
			this.inFlight.remove(key, entry);
			done.tryEmitEmpty();
		}).subscribe(message -> {
			// Freed before the response is queued: once the client has it, it may reuse
			// the id while this subscription has yet to reach doFinally.
			if (message instanceof JSONRPCResponse) {
				this.inFlight.remove(key, entry);
			}
			emit(message, entry);
		}));
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

	// Request ids are strings or integers, but a cancellation may reference one as any
	// number: 1, 1L and 1.0 must find the same entry, while 1.5 and "1" stay distinct.
	private static Object keyOf(Object id) {
		return id instanceof Number number && number.doubleValue() == number.longValue() ? (Object) number.longValue()
				: id;
	}

	private void emit(JSONRPCMessage message) {
		emit(message, null);
	}

	private void emit(JSONRPCMessage message, InFlight request) {
		// Output of requests that outlive shutdown has nowhere to go.
		if (this.shutDown.get()) {
			logger.debug("Dropping outbound message after shutdown: {}", message);
			return;
		}
		try {
			this.writer.schedule(() -> {
				// Checked when written, not when queued: a cancellation must also stop
				// output queued before it arrived, or emitted while it was processed.
				if (request != null && request.cancelled().get()) {
					logger.debug("Dropping outbound message of a cancelled request: {}", message);
					return;
				}
				writeLine(message);
			});
		}
		catch (RejectedExecutionException e) {
			// Passed the check just as shutdown disposed the writer.
			logger.debug("Dropping outbound message after shutdown: {}", message);
		}
	}

	private void writeLine(JSONRPCMessage message) {
		try {
			// JSON escapes line breaks inside strings, so raw ones are only whitespace,
			// e.g. from a pretty-printing mapper, and would split the message.
			String json = this.jsonMapper.writeValueAsString(message).replace("\n", "").replace("\r", "");
			// A single write, so nothing else writing to the stream can land between the
			// message and its newline.
			this.out.write((json + '\n').getBytes(StandardCharsets.UTF_8));
			this.out.flush();
		}
		catch (IOException | RuntimeException e) {
			logger.warn("Failed to write outbound message", e);
		}
	}

	private record InFlight(Disposable.Swap subscription, Mono<Void> done, AtomicBoolean cancelled) {
	}

}
