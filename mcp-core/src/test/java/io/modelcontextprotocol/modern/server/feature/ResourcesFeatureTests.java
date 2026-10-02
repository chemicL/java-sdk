/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.spec.json.gson.GsonMcpJsonMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static io.modelcontextprotocol.modern.server.ModernTestFixtures.SERVER_INFO;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.meta;
import static io.modelcontextprotocol.modern.server.ModernTestFixtures.respond;
import static org.assertj.core.api.Assertions.assertThat;

class ResourcesFeatureTests {

	@Test
	void unknownResourceIsRejectedAsInvalidParamsWithUri() {
		McpAsyncResourceRepository repo = new McpAsyncResourceRepository() {
			@Override
			public Mono<ResourcesPage> list(McpRequestContext ctx, String cursor) {
				return Mono.just(ResourcesPage.of(List.of()));
			}

			@Override
			public Mono<AsyncFeatureHandler<ReadResourceRequest, ReadResourceResult>> resolve(McpRequestContext ctx,
					String uri) {
				return Mono.empty();
			}
		};
		McpServer server = McpServer.builder()
			.serverInfo(SERVER_INFO)
			.jsonMapper(new GsonMcpJsonMapper())
			.feature(ResourcesFeature.of(repo, new GsonMcpJsonMapper(), 0L, McpSchema.CacheScope.PRIVATE))
			.build();
		Map<String, Object> meta = meta();
		JSONRPCRequest request = new JSONRPCRequest(McpSchema.METHOD_RESOURCES_READ, 1,
				Map.of("_meta", meta, "uri", "test://missing"));

		StepVerifier.create(respond(server, request)).assertNext(response -> {
			assertThat(response.error().code()).isEqualTo(ErrorCodes.INVALID_PARAMS);
			assertThat(response.error().data()).isEqualTo(Map.of("uri", "test://missing"));
		}).verifyComplete();
	}

}
