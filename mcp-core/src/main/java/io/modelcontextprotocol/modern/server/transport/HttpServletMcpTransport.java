/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.JsonRpc;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.modern.McpError;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.server.McpTransportContextExtractor;
import io.modelcontextprotocol.util.Assert;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

/**
 * A {@link jakarta.servlet.http.HttpServlet}-based transport for a modern
 * {@link McpRequestManager}. There is no session, no {@code GET} stream and no
 * {@code Mcp-Session-Id}: every POST is a self-contained request or notification.
 * <p>
 * Every request except {@code subscriptions/listen} is served start to finish by the
 * container thread that ran the filter chain: it is resolved with
 * {@link McpRequestManager#resolveBlocking} and the transport blocks on the result. Sync
 * repositories and handlers therefore run on that thread and see every thread-local a
 * servlet filter populated (Spring Security's {@code SecurityContextHolder}, MDC, ...). A
 * {@code Single} invocation is answered as {@code application/json}; a {@code Streaming}
 * one as {@code text/event-stream}, with each notification written and flushed before the
 * handler's notifier call returns.
 * <p>
 * {@code subscriptions/listen} is the exception: it runs no sync user code and can stay
 * open for hours, so it is resolved with {@link McpRequestManager#resolveNonBlocking} and
 * served over {@link AsyncContext}, releasing the container thread.
 *
 * @author Dariusz Jędrzejczyk
 */
@WebServlet(asyncSupported = true)
public class HttpServletMcpTransport extends HttpServlet {

	private static final int DEFAULT_REQUEST_MAX_SIZE = 16 * 1024 * 1024;

	private static final Logger logger = LoggerFactory.getLogger(HttpServletMcpTransport.class);

	private static final String UTF_8 = "UTF-8";

	private static final String APPLICATION_JSON = "application/json";

	private static final String TEXT_EVENT_STREAM = "text/event-stream";

	private final McpRequestManager requestManager;

	private final McpJsonMapper jsonMapper;

	private final String mcpEndpoint;

	private final McpTransportContextExtractor<HttpServletRequest> contextExtractor;

	private final int requestMaxSize;

	private volatile boolean closing = false;

	private HttpServletMcpTransport(McpRequestManager requestManager, McpJsonMapper jsonMapper, String mcpEndpoint,
			McpTransportContextExtractor<HttpServletRequest> contextExtractor, int requestMaxSize) {
		this.requestManager = requestManager;
		this.jsonMapper = jsonMapper;
		this.mcpEndpoint = mcpEndpoint;
		this.contextExtractor = contextExtractor;
		this.requestMaxSize = requestMaxSize;
	}

	public static Builder builder(McpRequestManager requestManager) {
		return new Builder(requestManager);
	}

	/**
	 * Stop accepting new requests. If the request handler is a {@link McpServer}, also
	 * asks it to end active {@code subscriptions/listen} streams gracefully.
	 */
	public void closeGracefully() {
		this.closing = true;
		if (this.requestManager instanceof io.modelcontextprotocol.modern.server.McpServer server) {
			server.closeGracefully();
		}
	}

	@Override
	public void destroy() {
		closeGracefully();
		super.destroy();
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		rejectLegacyVerb(request, response);
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		rejectLegacyVerb(request, response);
	}

	private void rejectLegacyVerb(HttpServletRequest request, HttpServletResponse response) throws IOException {
		if (!request.getRequestURI().endsWith(this.mcpEndpoint)) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		// Modern servers never mint sessions or resumable streams; a legacy GET/DELETE
		// gets a plain 405, per the four cheap obligations toward legacy traffic.
		response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		if (!request.getRequestURI().endsWith(this.mcpEndpoint)) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		if (this.closing) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Server is shutting down");
			return;
		}
		if (request.getContentLengthLong() > this.requestMaxSize) {
			response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
			return;
		}

		McpTransportContext transportContext = this.contextExtractor.extract(request);

		String body;
		try {
			body = readBody(request, this.requestMaxSize);
		}
		catch (BodyTooLargeException e) {
			response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
			return;
		}

		JSONRPCMessage message;
		try {
			message = JsonRpc.deserializeMessage(this.jsonMapper, body);
		}
		catch (IllegalArgumentException | IOException e) {
			writeJsonError(response, HttpServletResponse.SC_BAD_REQUEST,
					new JSONRPCError(ErrorCodes.PARSE_ERROR, "Invalid message format"));
			return;
		}

		if (message instanceof JSONRPCNotification notification) {
			this.requestManager.handleNotification(transportContext, notification).block();
			response.setStatus(HttpServletResponse.SC_ACCEPTED);
			return;
		}

		if (!(message instanceof JSONRPCRequest jsonRpcRequest)) {
			writeJsonError(response, HttpServletResponse.SC_BAD_REQUEST, new JSONRPCError(ErrorCodes.INVALID_REQUEST,
					"The server accepts either requests or notifications"));
			return;
		}

		String headerMismatch = validateHeaders(request, jsonRpcRequest);
		if (headerMismatch != null) {
			writeJsonRpcErrorResponse(response, jsonRpcRequest.id(),
					new JSONRPCError(ErrorCodes.HEADER_MISMATCH, headerMismatch));
			return;
		}

		boolean listen = io.modelcontextprotocol.modern.McpSchema.METHOD_SUBSCRIPTIONS_LISTEN
			.equals(jsonRpcRequest.method());
		McpInvocation invocation = (listen ? this.requestManager.resolveNonBlocking(transportContext, jsonRpcRequest)
				: this.requestManager.resolveBlocking(transportContext, jsonRpcRequest))
			.block();

		if (invocation instanceof McpInvocation.Streaming streaming) {
			if (listen) {
				streamOverAsyncContext(request, response, streaming);
			}
			else {
				streamOnContainerThread(response, streaming);
			}
			return;
		}

		JSONRPCResponse jsonRpcResponse = ((McpInvocation.Single) invocation).response().block();
		writeSingleResponse(response, jsonRpcResponse);
	}

	/**
	 * Subscribes on, and blocks, the container thread: a sync handler runs inside this
	 * subscription, and each notification it emits is written by the same thread before
	 * its notifier call returns. A client disconnect surfaces as a failed write, which
	 * cancels the stream; blocking handler code cannot be interrupted, so it runs to
	 * completion and its remaining output is discarded.
	 */
	private void streamOnContainerThread(HttpServletResponse response, McpInvocation.Streaming streaming)
			throws IOException {
		PrintWriter writer = startEventStream(response);
		try {
			streaming.messages().doOnNext(message -> writeEvent(writer, message)).blockLast();
		}
		catch (RuntimeException ex) {
			logger.debug("Streaming response ended early: {}", ex.getMessage());
		}
	}

	private void streamOverAsyncContext(HttpServletRequest request, HttpServletResponse response,
			McpInvocation.Streaming streaming) throws IOException {
		PrintWriter writer = startEventStream(response);
		AsyncContext asyncContext = request.startAsync();
		asyncContext.setTimeout(0);

		Disposable subscription = streaming.messages()
			.doOnNext(message -> writeEvent(writer, message))
			.subscribe(null, error -> {
				logger.debug("Listen stream ended early: {}", error.getMessage());
				asyncContext.complete();
			}, asyncContext::complete);

		asyncContext.addListener(new AsyncListener() {
			@Override
			public void onComplete(AsyncEvent event) {
			}

			@Override
			public void onTimeout(AsyncEvent event) {
				subscription.dispose();
			}

			@Override
			public void onError(AsyncEvent event) {
				subscription.dispose();
			}

			@Override
			public void onStartAsync(AsyncEvent event) {
			}
		});
	}

	private PrintWriter startEventStream(HttpServletResponse response) throws IOException {
		response.setContentType(TEXT_EVENT_STREAM);
		response.setCharacterEncoding(UTF_8);
		response.setHeader("Cache-Control", "no-cache");
		response.setHeader("X-Accel-Buffering", "no");
		response.setStatus(HttpServletResponse.SC_OK);
		response.flushBuffer();
		return response.getWriter();
	}

	/**
	 * Writes and flushes one SSE event. {@link PrintWriter} swallows I/O errors, so a
	 * closed client is detected via {@link PrintWriter#checkError()} and rethrown to end
	 * the stream.
	 */
	private void writeEvent(PrintWriter writer, JSONRPCMessage message) {
		String json;
		try {
			json = this.jsonMapper.writeValueAsString(message);
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
		writer.write("event: message\ndata: " + json + "\n\n");
		writer.flush();
		if (writer.checkError()) {
			throw new UncheckedIOException(new IOException("Client disconnected"));
		}
	}

	private void writeSingleResponse(HttpServletResponse response, JSONRPCResponse jsonRpcResponse) throws IOException {
		int status = jsonRpcResponse.error() != null ? httpStatusFor(jsonRpcResponse.error().code())
				: HttpServletResponse.SC_OK;
		response.setContentType(APPLICATION_JSON);
		response.setCharacterEncoding(UTF_8);
		response.setStatus(status);
		PrintWriter writer = response.getWriter();
		writer.write(this.jsonMapper.writeValueAsString(jsonRpcResponse));
		writer.flush();
	}

	private static int httpStatusFor(int jsonRpcErrorCode) {
		return switch (jsonRpcErrorCode) {
			case ErrorCodes.INVALID_PARAMS, ErrorCodes.MISSING_REQUIRED_CLIENT_CAPABILITY,
					ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION, ErrorCodes.HEADER_MISMATCH, ErrorCodes.INVALID_REQUEST,
					ErrorCodes.PARSE_ERROR ->
				HttpServletResponse.SC_BAD_REQUEST;
			case ErrorCodes.METHOD_NOT_FOUND -> HttpServletResponse.SC_NOT_FOUND;
			default -> HttpServletResponse.SC_OK;
		};
	}

	private void writeJsonError(HttpServletResponse response, int status, JSONRPCError error) throws IOException {
		response.setContentType(APPLICATION_JSON);
		response.setCharacterEncoding(UTF_8);
		response.setStatus(status);
		PrintWriter writer = response.getWriter();
		writer.write(this.jsonMapper.writeValueAsString(new McpError(error)));
		writer.flush();
	}

	private void writeJsonRpcErrorResponse(HttpServletResponse response, Object id, JSONRPCError error)
			throws IOException {
		writeSingleResponse(response, JSONRPCResponse.error(id, error));
	}

	/**
	 * Validates {@code Mcp-Method} and, for the three named methods, {@code Mcp-Name}
	 * against the request body. {@code MCP-Protocol-Version} is left to
	 * {@code McpServer}'s {@code _meta} validation, since a malformed body may not even
	 * carry a readable version yet.
	 * @return a human-readable mismatch description, or {@code null} if the headers are
	 * consistent with the body
	 */
	private String validateHeaders(HttpServletRequest request, JSONRPCRequest jsonRpcRequest) {
		String methodHeader = request.getHeader("Mcp-Method");
		if (methodHeader == null) {
			return "Missing required header: Mcp-Method";
		}
		if (!methodHeader.equals(jsonRpcRequest.method())) {
			return "Mcp-Method header does not match request method";
		}

		boolean nameRequired = io.modelcontextprotocol.modern.McpSchema.METHOD_TOOLS_CALL
			.equals(jsonRpcRequest.method())
				|| io.modelcontextprotocol.modern.McpSchema.METHOD_RESOURCES_READ.equals(jsonRpcRequest.method())
				|| io.modelcontextprotocol.modern.McpSchema.METHOD_PROMPTS_GET.equals(jsonRpcRequest.method());
		if (!nameRequired) {
			return null;
		}

		Object paramsObj = jsonRpcRequest.params();
		if (!(paramsObj instanceof Map<?, ?> paramsMap)) {
			return null;
		}
		Object expected = paramsMap.get("name");
		if (expected == null) {
			expected = paramsMap.get("uri");
		}
		if (!(expected instanceof String expectedName)) {
			return null;
		}
		String nameHeader = request.getHeader("Mcp-Name");
		if (nameHeader == null) {
			return isPlainAscii(expectedName) ? "Missing required header: Mcp-Name" : null;
		}
		String decoded = decodeMcpNameHeader(nameHeader);
		if (!expectedName.equals(decoded)) {
			return "Mcp-Name header does not match request name/uri";
		}
		return null;
	}

	private static boolean isPlainAscii(String value) {
		return value.chars().allMatch(c -> c < 128);
	}

	private static String decodeMcpNameHeader(String value) {
		if (value.startsWith("=?base64?") && value.endsWith("?=")) {
			String base64 = value.substring("=?base64?".length(), value.length() - "?=".length());
			return new String(java.util.Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
		}
		return value;
	}

	private static String readBody(HttpServletRequest request, int maxSize) throws IOException, BodyTooLargeException {
		InputStream inputStream = request.getInputStream();
		ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
		byte[] buf = new byte[8192];
		int totalBytes = 0;
		int readBytes;
		while ((readBytes = inputStream.read(buf, 0, buf.length)) != -1) {
			totalBytes += readBytes;
			if (totalBytes > maxSize) {
				throw new BodyTooLargeException();
			}
			bodyBytes.write(buf, 0, readBytes);
		}
		String charset = request.getCharacterEncoding() != null ? request.getCharacterEncoding()
				: StandardCharsets.UTF_8.name();
		return bodyBytes.toString(charset);
	}

	private static final class BodyTooLargeException extends Exception {

	}

	public static final class Builder {

		private final McpRequestManager requestManager;

		private McpJsonMapper jsonMapper;

		private String mcpEndpoint = "/mcp";

		private McpTransportContextExtractor<HttpServletRequest> contextExtractor = request -> McpTransportContext.EMPTY;

		private int requestMaxSize = DEFAULT_REQUEST_MAX_SIZE;

		private Builder(McpRequestManager requestManager) {
			Assert.notNull(requestManager, "requestManager must not be null");
			this.requestManager = requestManager;
		}

		public Builder jsonMapper(McpJsonMapper jsonMapper) {
			this.jsonMapper = jsonMapper;
			return this;
		}

		public Builder endpoint(String mcpEndpoint) {
			Assert.hasText(mcpEndpoint, "mcpEndpoint must not be empty");
			this.mcpEndpoint = mcpEndpoint;
			return this;
		}

		/**
		 * Extracts the {@link McpTransportContext} handlers see through
		 * {@code McpRequestContext#transportContext()}. Invoked once per POST, on the
		 * container thread, before the request is resolved - so it can capture
		 * thread-local state for async code, which may run elsewhere.
		 */
		public Builder contextExtractor(McpTransportContextExtractor<HttpServletRequest> contextExtractor) {
			Assert.notNull(contextExtractor, "contextExtractor must not be null");
			this.contextExtractor = contextExtractor;
			return this;
		}

		public Builder maxRequestSize(int requestMaxSize) {
			Assert.isTrue(requestMaxSize > 0, "requestMaxSize must be positive");
			this.requestMaxSize = requestMaxSize;
			return this;
		}

		public HttpServletMcpTransport build() {
			McpJsonMapper mapper = this.jsonMapper != null ? this.jsonMapper : McpJsonDefaults.getMapper();
			return new HttpServletMcpTransport(this.requestManager, mapper, this.mcpEndpoint, this.contextExtractor,
					this.requestMaxSize);
		}

	}

}
