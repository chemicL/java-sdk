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
import java.util.Base64;
import java.util.Collections;
import java.util.List;
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
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.McpInvocation;
import io.modelcontextprotocol.modern.server.McpRequestManager;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.server.McpTransportContextExtractor;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import io.modelcontextprotocol.server.transport.HeaderAccessor;
import io.modelcontextprotocol.server.transport.ServerHttpHeaderValidator;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityException;
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
 * A {@link HttpServlet} transport for a modern {@link McpRequestManager}: stateless, POST
 * only, one self-contained request or notification per call. Requests are blocking and
 * served on the container thread, so thread-locals set by servlet filters are visible to
 * sync handlers; {@code subscriptions/listen} is served asynchronously.
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

	private final ServerHttpHeaderValidator httpHeaderValidator;

	private volatile boolean closing = false;

	private HttpServletMcpTransport(McpRequestManager requestManager, McpJsonMapper jsonMapper, String mcpEndpoint,
			McpTransportContextExtractor<HttpServletRequest> contextExtractor, int requestMaxSize,
			ServerHttpHeaderValidator httpHeaderValidator) {
		this.requestManager = requestManager;
		this.jsonMapper = jsonMapper;
		this.mcpEndpoint = mcpEndpoint;
		this.contextExtractor = contextExtractor;
		this.requestMaxSize = requestMaxSize;
		this.httpHeaderValidator = httpHeaderValidator;
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
		if (this.requestManager instanceof McpServer server) {
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
		// gets a plain 405, without a session or stream.
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
		try {
			this.httpHeaderValidator.validate(headerAccessor(request));
		}
		catch (ServerTransportSecurityException e) {
			response.sendError(e.getStatusCode(), e.getMessage());
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
		catch (IOException e) {
			writeJsonRpcErrorResponse(response, null, new JSONRPCError(ErrorCodes.PARSE_ERROR, "Parse error"));
			return;
		}
		catch (IllegalArgumentException e) {
			writeJsonRpcErrorResponse(response, null,
					new JSONRPCError(ErrorCodes.INVALID_REQUEST, "Invalid JSON-RPC message"));
			return;
		}

		if (message instanceof JSONRPCNotification notification) {
			this.requestManager.handleNotification(transportContext, notification).block();
			response.setStatus(HttpServletResponse.SC_ACCEPTED);
			return;
		}

		if (!(message instanceof JSONRPCRequest jsonRpcRequest)) {
			writeJsonRpcErrorResponse(response, null, new JSONRPCError(ErrorCodes.INVALID_REQUEST,
					"The server accepts either requests or notifications"));
			return;
		}

		String headerMismatch = validateHeaders(request, jsonRpcRequest);
		if (headerMismatch != null) {
			writeJsonRpcErrorResponse(response, jsonRpcRequest.id(),
					new JSONRPCError(ErrorCodes.HEADER_MISMATCH, headerMismatch));
			return;
		}

		boolean listen = McpSchema.METHOD_SUBSCRIPTIONS_LISTEN.equals(jsonRpcRequest.method());
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

	private void streamOnContainerThread(HttpServletResponse response, McpInvocation.Streaming streaming)
			throws IOException {
		PrintWriter writer = startEventStream(response);
		// Blocking on the container thread lets sync handlers run inside this
		// subscription, each notification written before its notifier call returns. A
		// client disconnect fails a write and cancels the stream, but blocking handler
		// code can't be interrupted: it runs to completion, its output discarded.
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
		// PrintWriter swallows I/O errors; checkError is how a closed client shows up.
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

	private void writeJsonRpcErrorResponse(HttpServletResponse response, Object id, JSONRPCError error)
			throws IOException {
		writeSingleResponse(response, JSONRPCResponse.error(id, error));
	}

	/**
	 * Validates {@code Mcp-Method}, {@code MCP-Protocol-Version} and, for the three named
	 * methods, {@code Mcp-Name} against the request body.
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

		// A body without a readable version is left to McpServer's _meta validation,
		// which answers it with -32602.
		String bodyVersion = protocolVersionFromMeta(jsonRpcRequest.params());
		if (bodyVersion != null) {
			String versionHeader = request.getHeader("MCP-Protocol-Version");
			if (versionHeader == null) {
				return "Missing required header: MCP-Protocol-Version";
			}
			if (!versionHeader.equals(bodyVersion)) {
				return "MCP-Protocol-Version header does not match _meta protocol version";
			}
		}

		boolean nameRequired = McpSchema.METHOD_TOOLS_CALL.equals(jsonRpcRequest.method())
				|| McpSchema.METHOD_RESOURCES_READ.equals(jsonRpcRequest.method())
				|| McpSchema.METHOD_PROMPTS_GET.equals(jsonRpcRequest.method());
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
			return "Missing required header: Mcp-Name";
		}
		String decoded = decodeMcpNameHeader(nameHeader);
		if (decoded == null) {
			return "Mcp-Name header has a malformed Base64 value";
		}
		if (!expectedName.equals(decoded)) {
			return "Mcp-Name header does not match request name/uri";
		}
		return null;
	}

	private static String protocolVersionFromMeta(Object params) {
		if (params instanceof Map<?, ?> paramsMap && paramsMap.get("_meta") instanceof Map<?, ?> meta
				&& meta.get(MetaKeys.PROTOCOL_VERSION) instanceof String version) {
			return version;
		}
		return null;
	}

	private static HeaderAccessor headerAccessor(HttpServletRequest request) {
		return new HeaderAccessor() {
			@Override
			public List<String> getHeader(String name) {
				return Collections.list(request.getHeaders(name));
			}

			@Override
			public List<String> getHeaderNames() {
				return Collections.list(request.getHeaderNames());
			}
		};
	}

	private static String decodeMcpNameHeader(String value) {
		if (value.startsWith("=?base64?") && value.endsWith("?=")) {
			String base64 = value.substring("=?base64?".length(), value.length() - "?=".length());
			try {
				return new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
			}
			catch (IllegalArgumentException ex) {
				return null;
			}
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

		// No allowed origins: requests without an Origin (non-browser clients) pass, any
		// cross-origin browser request is rejected until explicitly allowed.
		private ServerHttpHeaderValidator httpHeaderValidator = DefaultServerTransportSecurityValidator.builder()
			.build();

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

		/**
		 * Validates the headers of every POST before it is read, e.g. Host/Origin checks
		 * against DNS rebinding. A rejection is answered with the exception's status
		 * code. Defaults to rejecting every request that carries an {@code Origin}
		 * header.
		 */
		public Builder httpHeaderValidator(ServerHttpHeaderValidator httpHeaderValidator) {
			Assert.notNull(httpHeaderValidator, "httpHeaderValidator must not be null");
			this.httpHeaderValidator = httpHeaderValidator;
			return this;
		}

		public HttpServletMcpTransport build() {
			McpJsonMapper mapper = this.jsonMapper != null ? this.jsonMapper : McpJsonDefaults.getMapper();
			return new HttpServletMcpTransport(this.requestManager, mapper, this.mcpEndpoint, this.contextExtractor,
					this.requestMaxSize, this.httpHeaderValidator);
		}

	}

}
