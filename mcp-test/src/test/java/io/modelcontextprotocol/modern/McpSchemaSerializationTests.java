/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.modern.McpSchema.DiscoverResult;
import io.modelcontextprotocol.modern.McpSchema.ElicitUrlRequest;
import io.modelcontextprotocol.modern.McpSchema.InputRequest;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.ListToolsResult;
import io.modelcontextprotocol.modern.McpSchema.ResultType;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.McpSchema.SubscriptionFilter;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Serialization tests for {@code modern.McpSchema}, run against the real Jackson mapper
 * (unlike the mcp-core unit tests, which use a Gson test double since mcp-core has no
 * Jackson dependency in test scope).
 *
 * @author Dariusz Jędrzejczyk
 */
class McpSchemaSerializationTests {

	private final McpJsonMapper jsonMapper = McpJsonDefaults.getMapper();

	@Test
	void resultTypeDefaultsToComplete() throws Exception {
		ListToolsResult result = ListToolsResult.builder(List.of()).build();
		String json = this.jsonMapper.writeValueAsString(result);
		assertThat(json).contains("\"resultType\":\"complete\"");

		ListToolsResult roundTripped = this.jsonMapper.readValue(json, ListToolsResult.class);
		assertThat(roundTripped.resultType()).isEqualTo(ResultType.COMPLETE);
	}

	@Test
	void listToolsResultCarriesCacheHints() throws Exception {
		Tool tool = Tool.builder("echo", Map.of("type", "object")).build();
		ListToolsResult result = ListToolsResult.builder(List.of(tool))
			.ttlMs(60000)
			.cacheScope(CacheScope.PRIVATE)
			.build();
		String json = this.jsonMapper.writeValueAsString(result);
		assertThat(json).contains("\"ttlMs\":60000").contains("\"cacheScope\":\"private\"");
	}

	@Test
	void missingCacheHintsDeserializeToDocumentedDefaults() throws Exception {
		String json = "{\"tools\":[]}";
		ListToolsResult result = this.jsonMapper.readValue(json, ListToolsResult.class);
		assertThat(result.ttlMs()).isEqualTo(0L);
		assertThat(result.cacheScope()).isEqualTo(CacheScope.PRIVATE);
	}

	@Test
	void unknownFieldIsIgnored() throws Exception {
		String json = "{\"tools\":[],\"ttlMs\":0,\"cacheScope\":\"private\",\"somethingNew\":42}";
		ListToolsResult result = this.jsonMapper.readValue(json, ListToolsResult.class);
		assertThat(result.tools()).isEmpty();
	}

	@Test
	void inputRequiredResultRequiresInputRequestsOrRequestState() {
		assertThatIllegalArgumentException().isThrownBy(() -> new InputRequiredResult(null, null, null, null));
	}

	@Test
	void inputRequiredResultSerializesResultTypeInputRequired() throws Exception {
		InputRequiredResult result = InputRequiredResult.builder().requestState("s").build();
		String json = this.jsonMapper.writeValueAsString(result);
		assertThat(json).contains("\"resultType\":\"input_required\"");
	}

	@Test
	void elicitUrlRequestHasNoElicitationId() throws Exception {
		ElicitUrlRequest request = new ElicitUrlRequest("Please confirm", "https://example.com", null);
		String json = this.jsonMapper.writeValueAsString(request);
		assertThat(json).doesNotContain("elicitationId").contains("\"mode\":\"url\"");
	}

	@Test
	void inputRequestFactoriesProduceExpectedMethodNames() {
		assertThat(InputRequest.elicitUrl("m", "https://example.com").method())
			.isEqualTo(McpSchema.METHOD_ELICITATION_CREATE);
		assertThat(InputRequest.listRoots().method()).isEqualTo(McpSchema.METHOD_ROOTS_LIST);
	}

	@Test
	void discoverResultMatchesSpecExample() throws Exception {
		DiscoverResult result = DiscoverResult
			.builder(List.of(McpSchema.LATEST_PROTOCOL_VERSION), ServerCapabilities.builder().tools(false).build())
			.instructions("Use tools wisely")
			.ttlMs(3600000)
			.cacheScope(CacheScope.PUBLIC)
			.build();
		String json = this.jsonMapper.writeValueAsString(result);
		assertThat(json).contains("\"supportedVersions\":[\"" + McpSchema.LATEST_PROTOCOL_VERSION + "\"]")
			.contains("\"resultType\":\"complete\"")
			.contains("\"cacheScope\":\"public\"");
	}

	@Test
	void clientCapabilitiesEmptyElicitationMeansFormOnly() {
		ClientCapabilities caps = ClientCapabilities.builder()
			.elicitation(new ClientCapabilities.Elicitation(null, null))
			.build();
		assertThat(caps.supportsElicitationForm()).isTrue();
		assertThat(caps.supportsElicitationUrl()).isFalse();
	}

	@Test
	void subscriptionFilterRoundTrips() throws Exception {
		SubscriptionFilter filter = new SubscriptionFilter(true, null, true, List.of("file:///a.txt"));
		String json = this.jsonMapper.writeValueAsString(filter);
		SubscriptionFilter roundTripped = this.jsonMapper.readValue(json, SubscriptionFilter.class);
		assertThat(roundTripped.wantsToolsListChanged()).isTrue();
		assertThat(roundTripped.wantsPromptsListChanged()).isFalse();
		assertThat(roundTripped.resourceSubscriptionsOrEmpty()).containsExactly("file:///a.txt");
	}

	@Test
	void callToolResultDefaultsResultTypeOnDeserialize() throws Exception {
		String json = "{\"content\":[]}";
		CallToolResult result = this.jsonMapper.readValue(json, CallToolResult.class);
		assertThat(result.resultType()).isEqualTo(ResultType.COMPLETE);
	}

}
