/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.transport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.modern.server.feature.AsyncToolHandler;
import io.modelcontextprotocol.modern.server.feature.McpAsyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.ToolsPage;
import io.modelcontextprotocol.server.transport.TomcatTestUtil;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.util.ToolsUtils;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

class HttpServletMcpTransportIntegrationTests {

	private static final int PORT = TomcatTestUtil.findAvailablePort();

	private static final String ENDPOINT = "/mcp";

	private static Tomcat tomcat;

	private static final McpJsonMapper JSON_MAPPER = io.modelcontextprotocol.json.McpJsonDefaults.getMapper();

	private static final Tool ECHO_TOOL = Tool.builder("echo", ToolsUtils.EMPTY_JSON_SCHEMA).build();

	@BeforeAll
	static void startServer() {
		McpAsyncToolRepository repo = new McpAsyncToolRepository() {
			@Override
			public Mono<ToolsPage> list(io.modelcontextprotocol.modern.server.McpRequestContext ctx, String cursor) {
				return Mono.just(ToolsPage.of(List.of(ECHO_TOOL)));
			}

			@Override
			public Mono<AsyncToolHandler> resolve(io.modelcontextprotocol.modern.server.McpRequestContext ctx,
					String name) {
				if ("echo".equals(name)) {
					return Mono.just(AsyncToolHandler.of((c, req) -> Mono
						.just(CallToolResult.builder().addContent(new TextContent("echo:" + req.name())).build())));
				}
				if ("streamer".equals(name)) {
					return Mono
						.just(AsyncToolHandler.streaming((c, req, notifier) -> notifier.progress(1.0, 1.0, "done")
							.thenReturn(CallToolResult.builder().addContent(new TextContent("streamed")).build())));
				}
				return Mono.empty();
			}
		};

		McpServer server = McpServer.builder()
			.serverInfo(Implementation.builder("modern-test-server", "1.0.0").build())
			.jsonMapper(JSON_MAPPER)
			.tools(repo)
			.build();

		HttpServletMcpTransport transport = HttpServletMcpTransport.builder(server)
			.jsonMapper(JSON_MAPPER)
			.endpoint(ENDPOINT)
			.build();

		tomcat = TomcatTestUtil.createTomcatServer("", PORT, transport);
		try {
			tomcat.start();
		}
		catch (LifecycleException e) {
			throw new RuntimeException(e);
		}
	}

	@AfterAll
	static void stopServer() throws LifecycleException {
		if (tomcat != null) {
			tomcat.stop();
			tomcat.destroy();
		}
	}

	private static HttpRequest.Builder post(String method, Map<String, Object> params) throws IOException {
		Map<String, Object> body = new java.util.HashMap<>();
		body.put("jsonrpc", "2.0");
		body.put("id", 1);
		body.put("method", method);
		body.put("params", params);
		String json = JSON_MAPPER.writeValueAsString(body);
		return HttpRequest.newBuilder()
			.uri(URI.create("http://localhost:" + PORT + ENDPOINT))
			.header("Content-Type", "application/json")
			.header("Accept", "application/json, text/event-stream")
			.header("Mcp-Method", method)
			.POST(HttpRequest.BodyPublishers.ofString(json));
	}

	private static Map<String, Object> meta() {
		Map<String, Object> meta = new java.util.HashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, Map.of());
		return meta;
	}

	@Test
	void discoverReturnsSupportedVersions() throws Exception {
		HttpRequest request = post("server/discover", Map.of("_meta", meta())).build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(response.headers().firstValue("Content-Type"))
			.hasValueSatisfying(v -> assertThat(v).contains("application/json"));
		Map<String, Object> parsed = JSON_MAPPER.readValue(response.body(), new TypeRef<Map<String, Object>>() {
		});
		@SuppressWarnings("unchecked")
		Map<String, Object> result = (Map<String, Object>) parsed.get("result");
		assertThat(result.get("supportedVersions"))
			.isEqualTo(List.of(io.modelcontextprotocol.modern.McpSchema.LATEST_PROTOCOL_VERSION));
	}

	@Test
	void unaryToolCallReturnsJson() throws Exception {
		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("name", "echo");
		HttpRequest request = post("tools/call", params).header("Mcp-Name", "echo").build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(response.headers().firstValue("Content-Type"))
			.hasValueSatisfying(v -> assertThat(v).contains("application/json"));
	}

	@Test
	void streamingToolCallReturnsSse() throws Exception {
		Map<String, Object> meta = meta();
		meta.put(MetaKeys.PROGRESS_TOKEN, "tok-1");
		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta);
		params.put("name", "streamer");
		HttpRequest request = post("tools/call", params).header("Mcp-Name", "streamer").build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(response.headers().firstValue("Content-Type"))
			.hasValueSatisfying(v -> assertThat(v).contains("text/event-stream"));
		assertThat(response.body()).contains("notifications/progress");
		assertThat(response.body()).contains("\"result\"");
	}

	@Test
	void getIsRejected() throws Exception {
		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create("http://localhost:" + PORT + ENDPOINT))
			.GET()
			.build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
		assertThat(response.statusCode()).isEqualTo(405);
	}

	@Test
	void headerMismatchIsRejected() throws Exception {
		Map<String, Object> params = new java.util.HashMap<>();
		params.put("_meta", meta());
		params.put("name", "echo");
		HttpRequest request = post("tools/call", params).setHeader("Mcp-Method", "tools/list").build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

		assertThat(response.statusCode()).isEqualTo(400);
		Map<String, Object> parsed = JSON_MAPPER.readValue(response.body(), new TypeRef<Map<String, Object>>() {
		});
		@SuppressWarnings("unchecked")
		Map<String, Object> error = (Map<String, Object>) parsed.get("error");
		assertThat(((Number) error.get("code")).intValue()).isEqualTo(ErrorCodes.HEADER_MISMATCH);
	}

	@Test
	void missingMetaIsRejected() throws Exception {
		HttpRequest request = post("tools/list", Map.of()).build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
		assertThat(response.statusCode()).isEqualTo(400);
	}

	@Test
	void notificationIsAccepted() throws Exception {
		Map<String, Object> body = Map.of("jsonrpc", "2.0", "method", "notifications/cancelled", "params",
				Map.of("requestId", 1));
		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create("http://localhost:" + PORT + ENDPOINT))
			.header("Content-Type", "application/json")
			.header("Accept", "application/json, text/event-stream")
			.header("Mcp-Method", "notifications/cancelled")
			.POST(HttpRequest.BodyPublishers.ofString(JSON_MAPPER.writeValueAsString(body)))
			.build();
		HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
		assertThat(response.statusCode()).isEqualTo(202);
	}

}
