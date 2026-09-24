/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.modelcontextprotocol.spec.McpSchema.Content;
import io.modelcontextprotocol.spec.McpSchema.CreateMessageRequest;
import io.modelcontextprotocol.spec.McpSchema.ElicitFormRequest;
import io.modelcontextprotocol.spec.McpSchema.LoggingLevel;
import io.modelcontextprotocol.spec.McpSchema.Prompt;
import io.modelcontextprotocol.spec.McpSchema.PromptMessage;
import io.modelcontextprotocol.spec.McpSchema.Resource;
import io.modelcontextprotocol.spec.McpSchema.ResourceContents;
import io.modelcontextprotocol.spec.McpSchema.ResourceTemplate;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.modelcontextprotocol.util.Assert;

/**
 * Wire types for the MCP {@code 2026-07-28} ("modern") revision of the protocol: no
 * handshake, no session, per-request {@code _meta}, and a required {@code resultType} on
 * every result.
 * <p>
 * Types that are unchanged by this revision (content blocks, {@link Tool},
 * {@link Resource}, {@link Prompt} and friends, the JSON-RPC envelope) are <b>not</b>
 * redefined here; reference {@code io.modelcontextprotocol.spec.McpSchema} directly for
 * those. This class holds only what is new or reshaped in the modern revision. The legacy
 * {@code io.modelcontextprotocol.spec.McpSchema} is never modified by this class.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class McpSchema {

	private static final Logger logger = LoggerFactory.getLogger(McpSchema.class);

	private McpSchema() {
	}

	// ---------------------------
	// Protocol constants
	// ---------------------------

	/** The protocol revision implemented by this package. */
	public static final String LATEST_PROTOCOL_VERSION = "2026-07-28";

	public static final String METHOD_SERVER_DISCOVER = "server/discover";

	public static final String METHOD_TOOLS_LIST = "tools/list";

	public static final String METHOD_TOOLS_CALL = "tools/call";

	public static final String METHOD_RESOURCES_LIST = "resources/list";

	public static final String METHOD_RESOURCES_TEMPLATES_LIST = "resources/templates/list";

	public static final String METHOD_RESOURCES_READ = "resources/read";

	public static final String METHOD_PROMPTS_LIST = "prompts/list";

	public static final String METHOD_PROMPTS_GET = "prompts/get";

	public static final String METHOD_COMPLETION_COMPLETE = "completion/complete";

	public static final String METHOD_SUBSCRIPTIONS_LISTEN = "subscriptions/listen";

	public static final String METHOD_NOTIFICATION_CANCELLED = "notifications/cancelled";

	public static final String METHOD_NOTIFICATION_PROGRESS = "notifications/progress";

	public static final String METHOD_NOTIFICATION_MESSAGE = "notifications/message";

	public static final String METHOD_NOTIFICATION_SUBSCRIPTIONS_ACKNOWLEDGED = "notifications/subscriptions/acknowledged";

	public static final String METHOD_NOTIFICATION_RESOURCES_UPDATED = "notifications/resources/updated";

	public static final String METHOD_NOTIFICATION_TOOLS_LIST_CHANGED = "notifications/tools/list_changed";

	public static final String METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED = "notifications/prompts/list_changed";

	public static final String METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED = "notifications/resources/list_changed";

	/** MRTR input-request payload methods; these never carry a JSON-RPC id. */
	public static final String METHOD_ELICITATION_CREATE = "elicitation/create";

	public static final String METHOD_SAMPLING_CREATE_MESSAGE = "sampling/createMessage";

	public static final String METHOD_ROOTS_LIST = "roots/list";

	/**
	 * The reserved {@code _meta} key names used on the modern wire.
	 */
	public static final class MetaKeys {

		public static final String PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion";

		public static final String CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities";

		public static final String CLIENT_INFO = "io.modelcontextprotocol/clientInfo";

		public static final String LOG_LEVEL = "io.modelcontextprotocol/logLevel";

		public static final String SERVER_INFO = "io.modelcontextprotocol/serverInfo";

		public static final String SUBSCRIPTION_ID = "io.modelcontextprotocol/subscriptionId";

		public static final String PROGRESS_TOKEN = "progressToken";

		private MetaKeys() {
		}

	}

	/**
	 * JSON-RPC and MCP-specific error codes for the modern revision.
	 * <p>
	 * {@code -32002} ({@code RESOURCE_NOT_FOUND}) and {@code -32042}
	 * ({@code URL_ELICITATION_REQUIRED}) from the legacy revision must never be emitted
	 * on the modern path; a missing resource is reported as {@link #INVALID_PARAMS}.
	 */
	public static final class ErrorCodes {

		public static final int PARSE_ERROR = -32700;

		public static final int INVALID_REQUEST = -32600;

		public static final int METHOD_NOT_FOUND = -32601;

		public static final int INVALID_PARAMS = -32602;

		public static final int INTERNAL_ERROR = -32603;

		public static final int HEADER_MISMATCH = -32020;

		public static final int MISSING_REQUIRED_CLIENT_CAPABILITY = -32021;

		public static final int UNSUPPORTED_PROTOCOL_VERSION = -32022;

		private ErrorCodes() {
		}

	}

	// ---------------------------
	// Result base types
	// ---------------------------

	/**
	 * Base type for every modern result. {@code resultType} is spec-required and is
	 * either {@link ResultType#COMPLETE} or {@link ResultType#INPUT_REQUIRED}; it is
	 * intentionally an open {@code String} rather than an enum, since future revisions
	 * may add values.
	 */
	public interface Result {

		String resultType();

		Map<String, Object> meta();

	}

	/**
	 * The two {@code resultType} values defined by this revision.
	 */
	public static final class ResultType {

		public static final String COMPLETE = "complete";

		public static final String INPUT_REQUIRED = "input_required";

		private ResultType() {
		}

	}

	/**
	 * A result that carries caching hints. Required on {@code resultType: "complete"}
	 * results from the list methods, {@code resources/read} and {@code server/discover};
	 * never present on an {@code "input_required"} result.
	 */
	public interface CacheableResult extends Result {

		Long ttlMs();

		CacheScope cacheScope();

	}

	/**
	 * Who may reuse a cached response. {@code PUBLIC} asserts the response contains no
	 * user-specific data and may be served to any client across access tokens; it must
	 * never be the default. {@code PRIVATE} permits reuse only within the same
	 * authorization context.
	 */
	public enum CacheScope {

		@JsonProperty("public")
		PUBLIC, @JsonProperty("private")
		PRIVATE

	}

	// ---------------------------
	// Capabilities
	// ---------------------------

	/**
	 * Capabilities a client declares on every request via {@code _meta}.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ClientCapabilities( // @formatter:off
		@JsonProperty("experimental") Map<String, Object> experimental,
		@JsonProperty("roots") Roots roots,
		@JsonProperty("sampling") Sampling sampling,
		@JsonProperty("elicitation") Elicitation elicitation,
		@JsonProperty("extensions") Map<String, Map<String, Object>> extensions) { // @formatter:on

		public static final ClientCapabilities NONE = new ClientCapabilities(null, null, null, null, null);

		public boolean supportsRoots() {
			return this.roots != null;
		}

		public boolean supportsSampling() {
			return this.sampling != null;
		}

		public boolean supportsElicitationForm() {
			return this.elicitation != null && (this.elicitation.form() != null || this.elicitation.url() == null);
		}

		public boolean supportsElicitationUrl() {
			return this.elicitation != null && this.elicitation.url() != null;
		}

		public static Builder builder() {
			return new Builder();
		}

		/** Marker for roots support; deprecated by the spec. */
		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Roots() {
		}

		/** Marker for sampling support; deprecated by the spec. */
		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Sampling() {
		}

		/**
		 * Elicitation support. An empty object is equivalent to {@code form} only.
		 */
		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Elicitation(@JsonProperty("form") Form form, @JsonProperty("url") Url url) {

			@JsonInclude(JsonInclude.Include.NON_ABSENT)
			@JsonIgnoreProperties(ignoreUnknown = true)
			public record Form() {
			}

			@JsonInclude(JsonInclude.Include.NON_ABSENT)
			@JsonIgnoreProperties(ignoreUnknown = true)
			public record Url() {
			}

		}

		public static final class Builder {

			private Map<String, Object> experimental;

			private Roots roots;

			private Sampling sampling;

			private Elicitation elicitation;

			private Map<String, Map<String, Object>> extensions;

			public Builder experimental(Map<String, Object> experimental) {
				this.experimental = experimental;
				return this;
			}

			public Builder roots() {
				this.roots = new Roots();
				return this;
			}

			public Builder sampling() {
				this.sampling = new Sampling();
				return this;
			}

			public Builder elicitation(Elicitation elicitation) {
				this.elicitation = elicitation;
				return this;
			}

			public Builder extensions(Map<String, Map<String, Object>> extensions) {
				this.extensions = extensions;
				return this;
			}

			public ClientCapabilities build() {
				return new ClientCapabilities(experimental, roots, sampling, elicitation, extensions);
			}

		}
	}

	/**
	 * Capabilities a server advertises via {@code server/discover}. Populated by
	 * aggregating every registered {@code McpFeature}.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ServerCapabilities( // @formatter:off
		@JsonProperty("experimental") Map<String, Object> experimental,
		@JsonProperty("logging") Logging logging,
		@JsonProperty("completions") Completions completions,
		@JsonProperty("prompts") Prompts prompts,
		@JsonProperty("resources") Resources resources,
		@JsonProperty("tools") Tools tools,
		@JsonProperty("extensions") Map<String, Map<String, Object>> extensions) { // @formatter:on

		public static Builder builder() {
			return new Builder();
		}

		/** Present if the server accepts a per-request {@code logLevel}. Deprecated. */
		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Logging() {
		}

		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Completions() {
		}

		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Prompts(@JsonProperty("listChanged") Boolean listChanged) {
		}

		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Resources(@JsonProperty("subscribe") Boolean subscribe,
				@JsonProperty("listChanged") Boolean listChanged) {
		}

		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Tools(@JsonProperty("listChanged") Boolean listChanged) {
		}

		/**
		 * Builder that supports merging contributions from multiple features via
		 * {@link #merge(ServerCapabilities)}.
		 */
		public static final class Builder {

			private Map<String, Object> experimental;

			private Logging logging;

			private Completions completions;

			private Prompts prompts;

			private Resources resources;

			private Tools tools;

			private Map<String, Map<String, Object>> extensions = new HashMap<>();

			public Builder experimental(Map<String, Object> experimental) {
				this.experimental = experimental;
				return this;
			}

			public Builder logging() {
				this.logging = new Logging();
				return this;
			}

			public Builder completions() {
				this.completions = new Completions();
				return this;
			}

			public Builder prompts(Boolean listChanged) {
				this.prompts = new Prompts(listChanged);
				return this;
			}

			public Builder resources(Boolean subscribe, Boolean listChanged) {
				this.resources = new Resources(subscribe, listChanged);
				return this;
			}

			public Builder tools(Boolean listChanged) {
				this.tools = new Tools(listChanged);
				return this;
			}

			public Builder extension(String id, Map<String, Object> settings) {
				this.extensions.put(id, settings == null ? Map.of() : settings);
				return this;
			}

			public boolean hasTools() {
				return this.tools != null;
			}

			public boolean hasPrompts() {
				return this.prompts != null;
			}

			public boolean hasResources() {
				return this.resources != null;
			}

			/** Turns on {@code listChanged}/{@code subscribe} flags contributed later. */
			public Builder toolsListChanged(boolean listChanged) {
				if (this.tools != null) {
					this.tools = new Tools(listChanged);
				}
				return this;
			}

			public Builder promptsListChanged(boolean listChanged) {
				if (this.prompts != null) {
					this.prompts = new Prompts(listChanged);
				}
				return this;
			}

			public Builder resourcesSubscribe(boolean subscribe, boolean listChanged) {
				if (this.resources != null) {
					this.resources = new Resources(subscribe, listChanged);
				}
				return this;
			}

			public ServerCapabilities build() {
				return new ServerCapabilities(experimental, logging, completions, prompts, resources, tools,
						extensions.isEmpty() ? null : Map.copyOf(extensions));
			}

		}
	}

	// ---------------------------
	// server/discover
	// ---------------------------

	/**
	 * The response to {@code server/discover}. Cacheable; the client uses it to determine
	 * supported protocol versions and capabilities before issuing any other request.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record DiscoverResult( // @formatter:off
		@JsonProperty("supportedVersions") List<String> supportedVersions,
		@JsonProperty("capabilities") ServerCapabilities capabilities,
		@JsonProperty("instructions") String instructions,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public DiscoverResult {
			Assert.notNull(supportedVersions, "supportedVersions must not be null");
			Assert.notNull(capabilities, "capabilities must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static DiscoverResult fromJson(@JsonProperty("supportedVersions") List<String> supportedVersions,
				@JsonProperty("capabilities") ServerCapabilities capabilities,
				@JsonProperty("instructions") String instructions, @JsonProperty("ttlMs") Long ttlMs,
				@JsonProperty("cacheScope") CacheScope cacheScope, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			if (supportedVersions == null || capabilities == null || ttlMs == null || cacheScope == null) {
				logger.warn("DiscoverResult: missing required fields during deserialization; substituting defaults");
				supportedVersions = supportedVersions == null ? List.of() : supportedVersions;
				capabilities = capabilities == null ? ServerCapabilities.builder().build() : capabilities;
				ttlMs = ttlMs == null ? 0L : ttlMs;
				cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			}
			return new DiscoverResult(supportedVersions, capabilities, instructions, ttlMs, cacheScope, resultType,
					meta);
		}

		public static Builder builder(List<String> supportedVersions, ServerCapabilities capabilities) {
			return new Builder(supportedVersions, capabilities);
		}

		public static final class Builder {

			private final List<String> supportedVersions;

			private final ServerCapabilities capabilities;

			private String instructions;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<String> supportedVersions, ServerCapabilities capabilities) {
				Assert.notNull(supportedVersions, "supportedVersions must not be null");
				Assert.notNull(capabilities, "capabilities must not be null");
				this.supportedVersions = supportedVersions;
				this.capabilities = capabilities;
			}

			public Builder instructions(String instructions) {
				this.instructions = instructions;
				return this;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				Assert.notNull(cacheScope, "cacheScope must not be null");
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public DiscoverResult build() {
				return new DiscoverResult(supportedVersions, capabilities, instructions, ttlMs, cacheScope,
						ResultType.COMPLETE, meta);
			}

		}
	}

	// ---------------------------
	// Requests that may carry MRTR retry data
	// ---------------------------

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record CallToolRequest( // @formatter:off
		@JsonProperty("name") String name,
		@JsonProperty("arguments") Map<String, Object> arguments,
		@JsonProperty("inputResponses") Map<String, Object> inputResponses,
		@JsonProperty("requestState") String requestState,
		@JsonProperty("_meta") Map<String, Object> meta) { // @formatter:on

		public CallToolRequest {
			Assert.hasText(name, "name must not be empty");
		}

		@JsonCreator
		static CallToolRequest fromJson(@JsonProperty("name") String name,
				@JsonProperty("arguments") Map<String, Object> arguments,
				@JsonProperty("inputResponses") Map<String, Object> inputResponses,
				@JsonProperty("requestState") String requestState, @JsonProperty("_meta") Map<String, Object> meta) {
			if (name == null) {
				logger.warn("CallToolRequest: missing required field 'name' during deserialization, using default ''");
				name = "";
			}
			return new CallToolRequest(name, arguments, inputResponses, requestState, meta);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ReadResourceRequest( // @formatter:off
		@JsonProperty("uri") String uri,
		@JsonProperty("inputResponses") Map<String, Object> inputResponses,
		@JsonProperty("requestState") String requestState,
		@JsonProperty("_meta") Map<String, Object> meta) { // @formatter:on

		public ReadResourceRequest {
			Assert.hasText(uri, "uri must not be empty");
		}

		@JsonCreator
		static ReadResourceRequest fromJson(@JsonProperty("uri") String uri,
				@JsonProperty("inputResponses") Map<String, Object> inputResponses,
				@JsonProperty("requestState") String requestState, @JsonProperty("_meta") Map<String, Object> meta) {
			if (uri == null) {
				logger
					.warn("ReadResourceRequest: missing required field 'uri' during deserialization, using default ''");
				uri = "";
			}
			return new ReadResourceRequest(uri, inputResponses, requestState, meta);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record GetPromptRequest( // @formatter:off
		@JsonProperty("name") String name,
		@JsonProperty("arguments") Map<String, String> arguments,
		@JsonProperty("inputResponses") Map<String, Object> inputResponses,
		@JsonProperty("requestState") String requestState,
		@JsonProperty("_meta") Map<String, Object> meta) { // @formatter:on

		public GetPromptRequest {
			Assert.hasText(name, "name must not be empty");
		}

		@JsonCreator
		static GetPromptRequest fromJson(@JsonProperty("name") String name,
				@JsonProperty("arguments") Map<String, String> arguments,
				@JsonProperty("inputResponses") Map<String, Object> inputResponses,
				@JsonProperty("requestState") String requestState, @JsonProperty("_meta") Map<String, Object> meta) {
			if (name == null) {
				logger.warn("GetPromptRequest: missing required field 'name' during deserialization, using default ''");
				name = "";
			}
			return new GetPromptRequest(name, arguments, inputResponses, requestState, meta);
		}
	}

	/** {@code cursor}/{@code _meta} shared by every paginated list request. */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record PaginatedRequest(@JsonProperty("cursor") String cursor,
			@JsonProperty("_meta") Map<String, Object> meta) {
	}

	// ---------------------------
	// List / read / call / get results
	// ---------------------------

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ListToolsResult( // @formatter:off
		@JsonProperty("tools") List<Tool> tools,
		@JsonProperty("nextCursor") String nextCursor,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public ListToolsResult {
			Assert.notNull(tools, "tools must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static ListToolsResult fromJson(@JsonProperty("tools") List<Tool> tools,
				@JsonProperty("nextCursor") String nextCursor, @JsonProperty("ttlMs") Long ttlMs,
				@JsonProperty("cacheScope") CacheScope cacheScope, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			if (tools == null || ttlMs == null || cacheScope == null) {
				logger.warn("ListToolsResult: missing required fields during deserialization; substituting defaults");
			}
			tools = tools == null ? List.of() : tools;
			ttlMs = ttlMs == null ? 0L : ttlMs;
			cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			return new ListToolsResult(tools, nextCursor, ttlMs, cacheScope, resultType, meta);
		}

		public static Builder builder(List<Tool> tools) {
			return new Builder(tools);
		}

		public static final class Builder {

			private final List<Tool> tools;

			private String nextCursor;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<Tool> tools) {
				Assert.notNull(tools, "tools must not be null");
				this.tools = tools;
			}

			public Builder nextCursor(String nextCursor) {
				this.nextCursor = nextCursor;
				return this;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				Assert.notNull(cacheScope, "cacheScope must not be null");
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public ListToolsResult build() {
				return new ListToolsResult(tools, nextCursor, ttlMs, cacheScope, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ListResourcesResult( // @formatter:off
		@JsonProperty("resources") List<Resource> resources,
		@JsonProperty("nextCursor") String nextCursor,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public ListResourcesResult {
			Assert.notNull(resources, "resources must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static ListResourcesResult fromJson(@JsonProperty("resources") List<Resource> resources,
				@JsonProperty("nextCursor") String nextCursor, @JsonProperty("ttlMs") Long ttlMs,
				@JsonProperty("cacheScope") CacheScope cacheScope, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			resources = resources == null ? List.of() : resources;
			ttlMs = ttlMs == null ? 0L : ttlMs;
			cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			return new ListResourcesResult(resources, nextCursor, ttlMs, cacheScope, resultType, meta);
		}

		public static Builder builder(List<Resource> resources) {
			return new Builder(resources);
		}

		public static final class Builder {

			private final List<Resource> resources;

			private String nextCursor;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<Resource> resources) {
				Assert.notNull(resources, "resources must not be null");
				this.resources = resources;
			}

			public Builder nextCursor(String nextCursor) {
				this.nextCursor = nextCursor;
				return this;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public ListResourcesResult build() {
				return new ListResourcesResult(resources, nextCursor, ttlMs, cacheScope, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ListResourceTemplatesResult( // @formatter:off
		@JsonProperty("resourceTemplates") List<ResourceTemplate> resourceTemplates,
		@JsonProperty("nextCursor") String nextCursor,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public ListResourceTemplatesResult {
			Assert.notNull(resourceTemplates, "resourceTemplates must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static ListResourceTemplatesResult fromJson(
				@JsonProperty("resourceTemplates") List<ResourceTemplate> resourceTemplates,
				@JsonProperty("nextCursor") String nextCursor, @JsonProperty("ttlMs") Long ttlMs,
				@JsonProperty("cacheScope") CacheScope cacheScope, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			resourceTemplates = resourceTemplates == null ? List.of() : resourceTemplates;
			ttlMs = ttlMs == null ? 0L : ttlMs;
			cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			return new ListResourceTemplatesResult(resourceTemplates, nextCursor, ttlMs, cacheScope, resultType, meta);
		}

		public static Builder builder(List<ResourceTemplate> resourceTemplates) {
			return new Builder(resourceTemplates);
		}

		public static final class Builder {

			private final List<ResourceTemplate> resourceTemplates;

			private String nextCursor;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<ResourceTemplate> resourceTemplates) {
				Assert.notNull(resourceTemplates, "resourceTemplates must not be null");
				this.resourceTemplates = resourceTemplates;
			}

			public Builder nextCursor(String nextCursor) {
				this.nextCursor = nextCursor;
				return this;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public ListResourceTemplatesResult build() {
				return new ListResourceTemplatesResult(resourceTemplates, nextCursor, ttlMs, cacheScope,
						ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ListPromptsResult( // @formatter:off
		@JsonProperty("prompts") List<Prompt> prompts,
		@JsonProperty("nextCursor") String nextCursor,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public ListPromptsResult {
			Assert.notNull(prompts, "prompts must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static ListPromptsResult fromJson(@JsonProperty("prompts") List<Prompt> prompts,
				@JsonProperty("nextCursor") String nextCursor, @JsonProperty("ttlMs") Long ttlMs,
				@JsonProperty("cacheScope") CacheScope cacheScope, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			prompts = prompts == null ? List.of() : prompts;
			ttlMs = ttlMs == null ? 0L : ttlMs;
			cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			return new ListPromptsResult(prompts, nextCursor, ttlMs, cacheScope, resultType, meta);
		}

		public static Builder builder(List<Prompt> prompts) {
			return new Builder(prompts);
		}

		public static final class Builder {

			private final List<Prompt> prompts;

			private String nextCursor;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<Prompt> prompts) {
				Assert.notNull(prompts, "prompts must not be null");
				this.prompts = prompts;
			}

			public Builder nextCursor(String nextCursor) {
				this.nextCursor = nextCursor;
				return this;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public ListPromptsResult build() {
				return new ListPromptsResult(prompts, nextCursor, ttlMs, cacheScope, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ReadResourceResult( // @formatter:off
		@JsonProperty("contents") List<ResourceContents> contents,
		@JsonProperty("ttlMs") Long ttlMs,
		@JsonProperty("cacheScope") CacheScope cacheScope,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements CacheableResult { // @formatter:on

		public ReadResourceResult {
			Assert.notNull(contents, "contents must not be null");
			Assert.notNull(ttlMs, "ttlMs must not be null");
			Assert.notNull(cacheScope, "cacheScope must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static ReadResourceResult fromJson(@JsonProperty("contents") List<ResourceContents> contents,
				@JsonProperty("ttlMs") Long ttlMs, @JsonProperty("cacheScope") CacheScope cacheScope,
				@JsonProperty("resultType") String resultType, @JsonProperty("_meta") Map<String, Object> meta) {
			contents = contents == null ? List.of() : contents;
			ttlMs = ttlMs == null ? 0L : ttlMs;
			cacheScope = cacheScope == null ? CacheScope.PRIVATE : cacheScope;
			return new ReadResourceResult(contents, ttlMs, cacheScope, resultType, meta);
		}

		public static Builder builder(List<ResourceContents> contents) {
			return new Builder(contents);
		}

		public static final class Builder {

			private final List<ResourceContents> contents;

			private Long ttlMs = 0L;

			private CacheScope cacheScope = CacheScope.PRIVATE;

			private Map<String, Object> meta;

			private Builder(List<ResourceContents> contents) {
				Assert.notNull(contents, "contents must not be null");
				this.contents = contents;
			}

			public Builder ttlMs(long ttlMs) {
				this.ttlMs = ttlMs;
				return this;
			}

			public Builder cacheScope(CacheScope cacheScope) {
				this.cacheScope = cacheScope;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public ReadResourceResult build() {
				return new ReadResourceResult(contents, ttlMs, cacheScope, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record CallToolResult( // @formatter:off
		@JsonProperty("content") List<Content> content,
		@JsonProperty("structuredContent") Object structuredContent,
		@JsonProperty("isError") Boolean isError,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements Result { // @formatter:on

		public CallToolResult {
			Assert.notNull(content, "content must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static CallToolResult fromJson(@JsonProperty("content") List<Content> content,
				@JsonProperty("structuredContent") Object structuredContent, @JsonProperty("isError") Boolean isError,
				@JsonProperty("resultType") String resultType, @JsonProperty("_meta") Map<String, Object> meta) {
			content = content == null ? List.of() : content;
			return new CallToolResult(content, structuredContent, isError, resultType, meta);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private List<Content> content = new ArrayList<>();

			private Object structuredContent;

			private Boolean isError;

			private Map<String, Object> meta;

			public Builder content(List<Content> content) {
				Assert.notNull(content, "content must not be null");
				this.content = new ArrayList<>(content);
				return this;
			}

			public Builder addContent(Content contentItem) {
				Assert.notNull(contentItem, "contentItem must not be null");
				this.content.add(contentItem);
				return this;
			}

			public Builder structuredContent(Object structuredContent) {
				this.structuredContent = structuredContent;
				return this;
			}

			public Builder isError(boolean isError) {
				this.isError = isError;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public CallToolResult build() {
				return new CallToolResult(content, structuredContent, isError, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record GetPromptResult( // @formatter:off
		@JsonProperty("description") String description,
		@JsonProperty("messages") List<PromptMessage> messages,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements Result { // @formatter:on

		public GetPromptResult {
			Assert.notNull(messages, "messages must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static GetPromptResult fromJson(@JsonProperty("description") String description,
				@JsonProperty("messages") List<PromptMessage> messages, @JsonProperty("resultType") String resultType,
				@JsonProperty("_meta") Map<String, Object> meta) {
			messages = messages == null ? List.of() : messages;
			return new GetPromptResult(description, messages, resultType, meta);
		}

		public static Builder builder(List<PromptMessage> messages) {
			return new Builder(messages);
		}

		public static final class Builder {

			private String description;

			private final List<PromptMessage> messages;

			private Map<String, Object> meta;

			private Builder(List<PromptMessage> messages) {
				Assert.notNull(messages, "messages must not be null");
				this.messages = messages;
			}

			public Builder description(String description) {
				this.description = description;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public GetPromptResult build() {
				return new GetPromptResult(description, messages, ResultType.COMPLETE, meta);
			}

		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record CompleteResult( // @formatter:off
		@JsonProperty("completion") Completion completion,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements Result { // @formatter:on

		public CompleteResult {
			Assert.notNull(completion, "completion must not be null");
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		@JsonCreator
		static CompleteResult fromJson(@JsonProperty("completion") Completion completion,
				@JsonProperty("resultType") String resultType, @JsonProperty("_meta") Map<String, Object> meta) {
			completion = completion == null ? new Completion(List.of(), null, null) : completion;
			return new CompleteResult(completion, resultType, meta);
		}

		public static CompleteResult of(Completion completion) {
			return new CompleteResult(completion, ResultType.COMPLETE, null);
		}

		@JsonInclude(JsonInclude.Include.NON_ABSENT)
		@JsonIgnoreProperties(ignoreUnknown = true)
		public record Completion( // @formatter:off
			@JsonProperty("values") List<String> values,
			@JsonProperty("total") Integer total,
			@JsonProperty("hasMore") Boolean hasMore) { // @formatter:on

			public Completion {
				Assert.notNull(values, "values must not be null");
			}

			public Completion(List<String> values) {
				this(values, null, null);
			}
		}
	}

	// ---------------------------
	// MRTR (Multi-Round Tool Response) - input-required results and payloads
	// ---------------------------

	/**
	 * One server-requested input, keyed by a server-assigned name inside
	 * {@link InputRequiredResult#inputRequests()}. {@code params} is one of
	 * {@link ElicitFormRequest}, {@link ElicitUrlRequest}, {@link CreateMessageRequest}
	 * or a bare "list roots" marker (an empty object).
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record InputRequest(@JsonProperty("method") String method, @JsonProperty("params") Object params) {

		public InputRequest {
			Assert.hasText(method, "method must not be empty");
		}

		public static InputRequest elicit(ElicitFormRequest request) {
			return new InputRequest(McpSchema.METHOD_ELICITATION_CREATE, request);
		}

		public static InputRequest elicitUrl(String message, String url) {
			return new InputRequest(McpSchema.METHOD_ELICITATION_CREATE, new ElicitUrlRequest(message, url, null));
		}

		public static InputRequest createMessage(CreateMessageRequest request) {
			return new InputRequest(McpSchema.METHOD_SAMPLING_CREATE_MESSAGE, request);
		}

		public static InputRequest listRoots() {
			return new InputRequest(McpSchema.METHOD_ROOTS_LIST, Map.of());
		}
	}

	/**
	 * A URL-mode elicitation request. Unlike the legacy revision's
	 * {@code ElicitUrlRequest}, this one carries no {@code elicitationId}; the retry
	 * correlates through the {@link InputRequiredResult}'s server-assigned key instead.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ElicitUrlRequest( // @formatter:off
		@JsonProperty("message") String message,
		@JsonProperty("url") String url,
		@JsonProperty("_meta") Map<String, Object> meta) { // @formatter:on

		public static final String MODE = "url";

		public ElicitUrlRequest {
			Assert.notNull(message, "message must not be null");
			Assert.notNull(url, "url must not be null");
		}

		@JsonProperty("mode")
		public String mode() {
			return MODE;
		}
	}

	/**
	 * A modern result signalling that the server needs more input before it can complete
	 * the original request. Only {@code tools/call}, {@code resources/read} and
	 * {@code prompts/get} may return this; that restriction is enforced by
	 * {@code McpServer} at dispatch time, not by the type system, since the set of
	 * methods that support MRTR is a spec detail that may change.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record InputRequiredResult( // @formatter:off
		@JsonProperty("inputRequests") Map<String, InputRequest> inputRequests,
		@JsonProperty("requestState") String requestState,
		@JsonProperty("resultType") String resultType,
		@JsonProperty("_meta") Map<String, Object> meta) implements Result { // @formatter:on

		public InputRequiredResult {
			Assert.isTrue(inputRequests != null || requestState != null,
					"at least one of inputRequests or requestState must be present");
			resultType = ResultType.INPUT_REQUIRED;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private final Map<String, InputRequest> inputRequests = new HashMap<>();

			private String requestState;

			private Map<String, Object> meta;

			public Builder elicit(String key, ElicitFormRequest request) {
				this.inputRequests.put(key, InputRequest.elicit(request));
				return this;
			}

			public Builder elicitUrl(String key, String message, String url) {
				this.inputRequests.put(key, InputRequest.elicitUrl(message, url));
				return this;
			}

			public Builder createMessage(String key, CreateMessageRequest request) {
				this.inputRequests.put(key, InputRequest.createMessage(request));
				return this;
			}

			public Builder listRoots(String key) {
				this.inputRequests.put(key, InputRequest.listRoots());
				return this;
			}

			public Builder requestState(String requestState) {
				this.requestState = requestState;
				return this;
			}

			public Builder meta(Map<String, Object> meta) {
				this.meta = meta;
				return this;
			}

			public InputRequiredResult build() {
				return new InputRequiredResult(inputRequests.isEmpty() ? null : Map.copyOf(inputRequests), requestState,
						ResultType.INPUT_REQUIRED, meta);
			}

		}
	}

	// ---------------------------
	// Subscriptions
	// ---------------------------

	/**
	 * The set of change notifications a client opts into via
	 * {@code subscriptions/listen}.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record SubscriptionFilter( // @formatter:off
		@JsonProperty("toolsListChanged") Boolean toolsListChanged,
		@JsonProperty("promptsListChanged") Boolean promptsListChanged,
		@JsonProperty("resourcesListChanged") Boolean resourcesListChanged,
		@JsonProperty("resourceSubscriptions") List<String> resourceSubscriptions) { // @formatter:on

		public static final SubscriptionFilter EMPTY = new SubscriptionFilter(null, null, null, null);

		public boolean wantsToolsListChanged() {
			return Boolean.TRUE.equals(this.toolsListChanged);
		}

		public boolean wantsPromptsListChanged() {
			return Boolean.TRUE.equals(this.promptsListChanged);
		}

		public boolean wantsResourcesListChanged() {
			return Boolean.TRUE.equals(this.resourcesListChanged);
		}

		public List<String> resourceSubscriptionsOrEmpty() {
			return this.resourceSubscriptions == null ? List.of() : this.resourceSubscriptions;
		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record SubscriptionsListenRequest(@JsonProperty("notifications") SubscriptionFilter notifications,
			@JsonProperty("_meta") Map<String, Object> meta) {

		@JsonCreator
		static SubscriptionsListenRequest fromJson(@JsonProperty("notifications") SubscriptionFilter notifications,
				@JsonProperty("_meta") Map<String, Object> meta) {
			return new SubscriptionsListenRequest(notifications == null ? SubscriptionFilter.EMPTY : notifications,
					meta);
		}
	}

	/**
	 * Params of {@code notifications/subscriptions/acknowledged}: the honoured subset.
	 */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record SubscriptionsAcknowledgedParams(@JsonProperty("notifications") SubscriptionFilter notifications,
			@JsonProperty("_meta") Map<String, Object> meta) {
	}

	/** Params of {@code notifications/resources/updated}. */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ResourceUpdatedParams(@JsonProperty("uri") String uri,
			@JsonProperty("_meta") Map<String, Object> meta) {

		public ResourceUpdatedParams {
			Assert.hasText(uri, "uri must not be empty");
		}
	}

	/** Params of {@code notifications/{tools,prompts,resources}/list_changed}. */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ListChangedParams(@JsonProperty("_meta") Map<String, Object> meta) {

		public static final ListChangedParams EMPTY = new ListChangedParams(null);

	}

	/** The terminal result of a server-closed {@code subscriptions/listen} stream. */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record SubscriptionsListenResult(@JsonProperty("resultType") String resultType,
			@JsonProperty("_meta") Map<String, Object> meta) implements Result {

		public SubscriptionsListenResult {
			resultType = resultType == null ? ResultType.COMPLETE : resultType;
		}

		public static SubscriptionsListenResult forSubscription(Object subscriptionId) {
			Map<String, Object> meta = new HashMap<>();
			meta.put(MetaKeys.SUBSCRIPTION_ID, subscriptionId);
			return new SubscriptionsListenResult(ResultType.COMPLETE, meta);
		}
	}

	// ---------------------------
	// Other notifications
	// ---------------------------

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record CancelledNotificationParams(@JsonProperty("requestId") Object requestId,
			@JsonProperty("reason") String reason) {

		public CancelledNotificationParams {
			Assert.notNull(requestId, "requestId must not be null");
		}

		@JsonCreator
		static CancelledNotificationParams fromJson(@JsonProperty("requestId") Object requestId,
				@JsonProperty("reason") String reason) {
			if (requestId == null) {
				logger.warn(
						"CancelledNotificationParams: missing required field 'requestId' during deserialization, using default ''");
				requestId = "";
			}
			return new CancelledNotificationParams(requestId, reason);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ProgressParams( // @formatter:off
		@JsonProperty("progressToken") Object progressToken,
		@JsonProperty("progress") Double progress,
		@JsonProperty("total") Double total,
		@JsonProperty("message") String message,
		@JsonProperty("_meta") Map<String, Object> meta) { // @formatter:on

		public ProgressParams {
			Assert.notNull(progressToken, "progressToken must not be null");
			Assert.notNull(progress, "progress must not be null");
		}
	}

	/** Params of {@code notifications/message}; {@code data} is any JSON value. */
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record LoggingMessageParams(@JsonProperty("level") LoggingLevel level, @JsonProperty("logger") String logger,
			@JsonProperty("data") Object data) {

		public LoggingMessageParams {
			Assert.notNull(level, "level must not be null");
			Assert.notNull(data, "data must not be null");
		}
	}

	// ---------------------------
	// Error data payloads
	// ---------------------------

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record MissingRequiredClientCapabilityData(
			@JsonProperty("requiredCapabilities") ClientCapabilities requiredCapabilities) {
	}

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public record UnsupportedProtocolVersionData(@JsonProperty("supported") List<String> supported,
			@JsonProperty("requested") String requested) {
	}

}
