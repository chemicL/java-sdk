/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities.Elicitation;
import io.modelcontextprotocol.modern.McpSchema.ElicitUrlRequest;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.InputRequest;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.MissingRequiredClientCapabilityData;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.McpSchema.UnsupportedProtocolVersionData;
import io.modelcontextprotocol.modern.server.feature.CompletionsFeature;
import io.modelcontextprotocol.modern.server.feature.DiscoverFeature;
import io.modelcontextprotocol.modern.server.feature.McpAsyncCompletionRepository;
import io.modelcontextprotocol.modern.server.feature.McpAsyncPromptRepository;
import io.modelcontextprotocol.modern.server.feature.McpAsyncResourceRepository;
import io.modelcontextprotocol.modern.server.feature.McpAsyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.McpSyncCompletionRepository;
import io.modelcontextprotocol.modern.server.feature.McpSyncPromptRepository;
import io.modelcontextprotocol.modern.server.feature.McpSyncResourceRepository;
import io.modelcontextprotocol.modern.server.feature.McpChangeFeed;
import io.modelcontextprotocol.modern.server.feature.McpSyncToolRepository;
import io.modelcontextprotocol.modern.server.feature.PromptsFeature;
import io.modelcontextprotocol.modern.server.feature.ResourcesFeature;
import io.modelcontextprotocol.modern.server.feature.SubscriptionsFeature;
import io.modelcontextprotocol.modern.server.feature.ToolsFeature;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.spec.McpSchema.LoggingLevel;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * The composition root for a modern MCP server: a stateless, immutable dispatcher over a
 * composed {@link McpRouter} of {@link McpFeature}s, wrapped by any registered
 * {@link McpFilter}s.
 * <p>
 * {@code McpServer} owns exactly what must live in one place: {@code _meta} validation
 * and version negotiation, {@code server/discover}, {@code serverInfo} stamping, error
 * mapping, and (once configured) {@code requestState} sealing for MRTR. Everything else -
 * tool, resource and prompt handling, subscriptions, extensions - is a feature. There is
 * no {@code McpSyncServer} twin: sync and async are properties of repositories and
 * handlers, not of the server.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class McpServer implements McpRequestHandler {

	private static final Logger logger = LoggerFactory.getLogger(McpServer.class);

	private static final TypeRef<Map<String, Object>> MAP_TYPE_REF = new TypeRef<>() {
	};

	private final Implementation serverInfo;

	private final List<String> supportedVersions;

	private final McpJsonMapper jsonMapper;

	private final McpRouter dispatchChain;

	private final Set<String> inputRequiredMethods;

	private final RequestStateCodec requestStateCodec;

	private final SubscriptionsFeature subscriptionsFeature;

	private static final Set<String> MRTR_ELIGIBLE_METHODS = Set.of(McpSchema.METHOD_TOOLS_CALL,
			McpSchema.METHOD_RESOURCES_READ, McpSchema.METHOD_PROMPTS_GET);

	private McpServer(Implementation serverInfo, List<String> supportedVersions, McpJsonMapper jsonMapper,
			McpRouter router, List<McpFilter> filters, Set<String> inputRequiredMethods,
			RequestStateCodec requestStateCodec, SubscriptionsFeature subscriptionsFeature) {
		this.serverInfo = serverInfo;
		this.supportedVersions = supportedVersions;
		this.jsonMapper = jsonMapper;
		this.inputRequiredMethods = inputRequiredMethods;
		this.requestStateCodec = requestStateCodec;
		this.subscriptionsFeature = subscriptionsFeature;
		McpRouter chain = router;
		for (int i = filters.size() - 1; i >= 0; i--) {
			McpFilter filter = filters.get(i);
			McpRouter next = chain;
			chain = ctx -> filter.filter(ctx, next);
		}
		this.dispatchChain = chain;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Ends every active {@code subscriptions/listen} stream with a graceful
	 * {@code complete} result. A no-op if no {@link McpChangeFeed} was registered.
	 */
	public void closeGracefully() {
		if (this.subscriptionsFeature != null) {
			this.subscriptionsFeature.closeGracefully();
		}
	}

	@Override
	public Mono<McpInvocation> resolveBlocking(McpTransportContext transportContext, JSONRPCRequest request) {
		return resolve(transportContext, request, true);
	}

	@Override
	public Mono<McpInvocation> resolveNonBlocking(McpTransportContext transportContext, JSONRPCRequest request) {
		return resolve(transportContext, request, false);
	}

	private Mono<McpInvocation> resolve(McpTransportContext transportContext, JSONRPCRequest request,
			boolean blocking) {
		return Mono.defer(() -> {
			Object id = request.id();
			try {
				return doResolve(transportContext, request, id, blocking);
			}
			catch (Exception ex) {
				return Mono.just(McpInvocation.unary(Mono.just(errorResponse(id, ex))));
			}
		});
	}

	private Mono<McpInvocation> doResolve(McpTransportContext transportContext, JSONRPCRequest request, Object id,
			boolean blocking) {
		Map<String, Object> paramsMap = toParamsMap(request.params());
		if (paramsMap == null) {
			return unaryError(id, ErrorCodes.INVALID_PARAMS, "params is required");
		}
		Map<String, Object> meta = asMetaMap(paramsMap.get("_meta"));
		if (meta == null) {
			return unaryError(id, ErrorCodes.INVALID_PARAMS, "params._meta is required");
		}
		Object versionRaw = meta.get(MetaKeys.PROTOCOL_VERSION);
		if (!(versionRaw instanceof String protocolVersion) || protocolVersion.isBlank()) {
			return unaryError(id, ErrorCodes.INVALID_PARAMS, "_meta['" + MetaKeys.PROTOCOL_VERSION + "'] is required");
		}
		Object capabilitiesRaw = meta.get(MetaKeys.CLIENT_CAPABILITIES);
		if (capabilitiesRaw == null) {
			return unaryError(id, ErrorCodes.INVALID_PARAMS,
					"_meta['" + MetaKeys.CLIENT_CAPABILITIES + "'] is required");
		}
		if (!this.supportedVersions.contains(protocolVersion)) {
			return unaryError(id, ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION, "Unsupported protocol version",
					new UnsupportedProtocolVersionData(this.supportedVersions, protocolVersion));
		}

		ClientCapabilities clientCapabilities = this.jsonMapper.convertValue(capabilitiesRaw, ClientCapabilities.class);
		Object clientInfoRaw = meta.get(MetaKeys.CLIENT_INFO);
		Implementation clientInfo = clientInfoRaw == null ? null
				: this.jsonMapper.convertValue(clientInfoRaw, Implementation.class);

		Object logLevelRaw = meta.get(MetaKeys.LOG_LEVEL);
		LoggingLevel logLevel = null;
		if (logLevelRaw != null) {
			logLevel = logLevelRaw instanceof String s ? LoggingLevel.fromValue(s) : null;
			if (logLevel == null) {
				return unaryError(id, ErrorCodes.INVALID_PARAMS, "_meta['" + MetaKeys.LOG_LEVEL + "'] is invalid");
			}
		}
		Object progressToken = meta.get(MetaKeys.PROGRESS_TOKEN);
		String primitiveName = extractPrimitiveName(paramsMap);
		boolean retry = paramsMap.get("inputResponses") != null || paramsMap.get("requestState") != null;

		McpRequestContext ctx = new McpRequestContext(id, request.method(), protocolVersion, clientCapabilities,
				clientInfo, logLevel, progressToken, primitiveName, meta, transportContext, retry, blocking);

		Map<String, Object> effectiveParams = paramsMap;
		if (retry && MRTR_ELIGIBLE_METHODS.contains(request.method())) {
			Object requestStateRaw = paramsMap.get("requestState");
			if (requestStateRaw instanceof String sealed) {
				effectiveParams = new LinkedHashMap<>(paramsMap);
				effectiveParams.put("requestState", this.requestStateCodec.open(ctx, sealed));
			}
		}
		Object finalParams = effectiveParams;

		return this.dispatchChain.route(ctx)
			.map(handler -> dispatch(ctx, handler, finalParams))
			.switchIfEmpty(Mono.fromSupplier(() -> unaryErrorInvocation(id, ErrorCodes.METHOD_NOT_FOUND,
					"Method not found: " + request.method(), null)))
			.onErrorResume(err -> Mono.just(unaryErrorInvocation(id, err)));
	}

	@Override
	public Mono<Void> handleNotification(McpTransportContext transportContext, JSONRPCNotification notification) {
		// The only modern client notification is notifications/cancelled, which
		// transports handle directly against their own in-flight bookkeeping.
		return Mono.empty();
	}

	private McpInvocation dispatch(McpRequestContext ctx, McpHandler handler, Object rawParams) {
		if (handler instanceof McpHandler.Streaming streaming) {
			return McpInvocation.streaming(buildStreamingFlux(ctx, streaming, rawParams));
		}
		Mono<JSONRPCResponse> response = handler.handle(ctx, rawParams)
			.map(result -> mapResultToResponse(ctx, result))
			.onErrorResume(err -> Mono.just(errorResponse(ctx.requestId(), err)));
		return McpInvocation.unary(response);
	}

	private Flux<JSONRPCMessage> buildStreamingFlux(McpRequestContext ctx, McpHandler.Streaming handler,
			Object rawParams) {
		Sinks.Many<JSONRPCNotification> sink = Sinks.many().unicast().onBackpressureBuffer();
		DefaultAsyncNotifier notifier = new DefaultAsyncNotifier(ctx, sink);
		Mono<JSONRPCMessage> terminal = handler.handle(ctx, rawParams, notifier)
			.map(result -> (JSONRPCMessage) mapResultToResponse(ctx, result))
			.onErrorResume(err -> Mono.just((JSONRPCMessage) errorResponse(ctx.requestId(), err)))
			.doFinally(signal -> sink.tryEmitComplete());
		return Flux.merge(sink.asFlux(), terminal.flux());
	}

	private JSONRPCResponse mapResultToResponse(McpRequestContext ctx, Result result) {
		if (result instanceof InputRequiredResult inputRequired) {
			if (!this.inputRequiredMethods.contains(ctx.method())) {
				throw McpError.builder(ErrorCodes.INTERNAL_ERROR)
					.message("Method '" + ctx.method() + "' must not answer with an input-required result")
					.build();
			}
			result = sealAndCheck(ctx, inputRequired);
		}
		Map<String, Object> resultMap = new LinkedHashMap<>(this.jsonMapper.convertValue(result, MAP_TYPE_REF));
		Map<String, Object> resultMeta = new LinkedHashMap<>();
		Object existingMeta = resultMap.get("_meta");
		if (existingMeta instanceof Map<?, ?> m) {
			m.forEach((k, v) -> resultMeta.put(String.valueOf(k), v));
		}
		resultMeta.put(MetaKeys.SERVER_INFO, this.serverInfo);
		resultMap.put("_meta", resultMeta);
		// A retry's response carries fulfilled MRTR state, never a cacheable snapshot.
		if (ctx.isRetry()) {
			resultMap.remove("ttlMs");
			resultMap.remove("cacheScope");
		}
		return JSONRPCResponse.result(ctx.requestId(), resultMap);
	}

	/**
	 * Verifies every {@code inputRequests} entry is covered by a client-declared
	 * capability, then seals {@code requestState} for the wire.
	 */
	private InputRequiredResult sealAndCheck(McpRequestContext ctx, InputRequiredResult inputRequired) {
		if (inputRequired.inputRequests() != null) {
			boolean needsElicitForm = false;
			boolean needsElicitUrl = false;
			boolean needsSampling = false;
			boolean needsRoots = false;
			for (InputRequest inputRequest : inputRequired.inputRequests().values()) {
				switch (inputRequest.method()) {
					case McpSchema.METHOD_ELICITATION_CREATE -> {
						if (inputRequest.params() instanceof ElicitUrlRequest) {
							needsElicitUrl = needsElicitUrl || !ctx.clientCapabilities().supportsElicitationUrl();
						}
						else {
							needsElicitForm = needsElicitForm || !ctx.clientCapabilities().supportsElicitationForm();
						}
					}
					case McpSchema.METHOD_SAMPLING_CREATE_MESSAGE ->
						needsSampling = needsSampling || !ctx.clientCapabilities().supportsSampling();
					case McpSchema.METHOD_ROOTS_LIST ->
						needsRoots = needsRoots || !ctx.clientCapabilities().supportsRoots();
					default -> {
					}
				}
			}
			if (needsElicitForm || needsElicitUrl || needsSampling || needsRoots) {
				ClientCapabilities.Builder missing = ClientCapabilities.builder();
				if (needsElicitForm || needsElicitUrl) {
					missing.elicitation(new Elicitation(needsElicitForm ? new Elicitation.Form() : null,
							needsElicitUrl ? new Elicitation.Url() : null));
				}
				if (needsSampling) {
					missing.sampling();
				}
				if (needsRoots) {
					missing.roots();
				}
				throw McpError.builder(ErrorCodes.MISSING_REQUIRED_CLIENT_CAPABILITY)
					.message("Missing required client capability for MRTR input request")
					.data(new MissingRequiredClientCapabilityData(missing.build()))
					.build();
			}
		}
		if (inputRequired.requestState() == null) {
			return inputRequired;
		}
		String sealed = this.requestStateCodec.seal(ctx, inputRequired.requestState());
		return new InputRequiredResult(inputRequired.inputRequests(), sealed, inputRequired.resultType(),
				inputRequired.meta());
	}

	private JSONRPCResponse errorResponse(Object id, Throwable throwable) {
		if (throwable instanceof McpError mcpError) {
			return JSONRPCResponse.error(id, mcpError.getJsonRpcError());
		}
		logger.warn("Unhandled exception while dispatching request {}", id, throwable);
		String message = throwable.getMessage() != null ? throwable.getMessage() : "Internal error";
		return JSONRPCResponse.error(id, new JSONRPCError(ErrorCodes.INTERNAL_ERROR, message));
	}

	private Mono<McpInvocation> unaryError(Object id, int code, String message) {
		return unaryError(id, code, message, null);
	}

	private Mono<McpInvocation> unaryError(Object id, int code, String message, Object data) {
		return Mono.just(unaryErrorInvocation(id, code, message, data));
	}

	private McpInvocation unaryErrorInvocation(Object id, int code, String message, Object data) {
		return McpInvocation.unary(Mono.just(JSONRPCResponse.error(id, new JSONRPCError(code, message, data))));
	}

	private McpInvocation unaryErrorInvocation(Object id, Throwable throwable) {
		return McpInvocation.unary(Mono.just(errorResponse(id, throwable)));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> toParamsMap(Object params) {
		if (params == null) {
			return null;
		}
		if (params instanceof Map<?, ?> m) {
			return (Map<String, Object>) m;
		}
		return this.jsonMapper.convertValue(params, MAP_TYPE_REF);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMetaMap(Object metaRaw) {
		if (metaRaw instanceof Map<?, ?> m) {
			return (Map<String, Object>) m;
		}
		return null;
	}

	private static String extractPrimitiveName(Map<String, Object> paramsMap) {
		Object name = paramsMap.get("name");
		if (name instanceof String s) {
			return s;
		}
		Object uri = paramsMap.get("uri");
		if (uri instanceof String s) {
			return s;
		}
		return null;
	}

	/**
	 * Builds an immutable {@link McpServer} from registered features and filters.
	 */
	public static final class Builder {

		private Implementation serverInfo;

		private String instructions;

		private List<String> supportedVersions = List.of(McpSchema.LATEST_PROTOCOL_VERSION);

		private McpJsonMapper jsonMapper;

		private final List<McpFeature> features = new ArrayList<>();

		private final List<McpFilter> filters = new ArrayList<>();

		private long defaultTtlMs = 0L;

		private CacheScope defaultCacheScope = CacheScope.PRIVATE;

		private Function<McpJsonMapper, McpFeature> toolsFeatureFactory;

		private Function<McpJsonMapper, McpFeature> resourcesFeatureFactory;

		private Function<McpJsonMapper, McpFeature> promptsFeatureFactory;

		private Function<McpJsonMapper, McpFeature> completionsFeatureFactory;

		private RequestStateCodec requestStateCodec;

		private McpChangeFeed changeFeed;

		private Builder() {
		}

		public Builder serverInfo(Implementation serverInfo) {
			this.serverInfo = serverInfo;
			return this;
		}

		public Builder instructions(String instructions) {
			this.instructions = instructions;
			return this;
		}

		public Builder supportedVersions(List<String> supportedVersions) {
			Assert.notEmpty(supportedVersions, "supportedVersions must not be empty");
			this.supportedVersions = List.copyOf(supportedVersions);
			return this;
		}

		public Builder jsonMapper(McpJsonMapper jsonMapper) {
			this.jsonMapper = jsonMapper;
			return this;
		}

		public Builder feature(McpFeature feature) {
			Assert.notNull(feature, "feature must not be null");
			this.features.add(feature);
			return this;
		}

		public Builder tools(McpAsyncToolRepository repository) {
			this.toolsFeatureFactory = mapper -> ToolsFeature.of(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder tools(McpSyncToolRepository repository) {
			this.toolsFeatureFactory = mapper -> ToolsFeature.ofSync(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder resources(McpAsyncResourceRepository repository) {
			this.resourcesFeatureFactory = mapper -> ResourcesFeature.of(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder resources(McpSyncResourceRepository repository) {
			this.resourcesFeatureFactory = mapper -> ResourcesFeature.ofSync(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder prompts(McpAsyncPromptRepository repository) {
			this.promptsFeatureFactory = mapper -> PromptsFeature.of(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder prompts(McpSyncPromptRepository repository) {
			this.promptsFeatureFactory = mapper -> PromptsFeature.ofSync(repository, mapper, this.defaultTtlMs,
					this.defaultCacheScope);
			return this;
		}

		public Builder completions(McpAsyncCompletionRepository repository) {
			this.completionsFeatureFactory = mapper -> CompletionsFeature.of(repository, mapper);
			return this;
		}

		public Builder completions(McpSyncCompletionRepository repository) {
			this.completionsFeatureFactory = mapper -> CompletionsFeature.ofSync(repository, mapper);
			return this;
		}

		/**
		 * Registers {@code subscriptions/listen}, backed by {@code feed}. Must see the
		 * final set of registered primitives to decide which change types it can honour,
		 * so it is wired up last, after every other feature.
		 */
		public Builder subscriptions(McpChangeFeed feed) {
			Assert.notNull(feed, "feed must not be null");
			this.changeFeed = feed;
			return this;
		}

		public Builder filter(McpFilter filter) {
			Assert.notNull(filter, "filter must not be null");
			this.filters.add(filter);
			return this;
		}

		/**
		 * The codec used to seal/open MRTR {@code requestState}. Defaults to
		 * {@link HmacRequestStateCodec#builder()}{@code .build()}.
		 */
		public Builder requestStateCodec(RequestStateCodec requestStateCodec) {
			Assert.notNull(requestStateCodec, "requestStateCodec must not be null");
			this.requestStateCodec = requestStateCodec;
			return this;
		}

		/**
		 * Default caching hints applied by features that don't set their own. Never
		 * default {@code cacheScope} to {@code PUBLIC}: doing so lets any client, gateway
		 * or proxy reuse a response across access tokens.
		 */
		public Builder cacheDefaults(long ttlMs, CacheScope cacheScope) {
			Assert.notNull(cacheScope, "cacheScope must not be null");
			this.defaultTtlMs = ttlMs;
			this.defaultCacheScope = cacheScope;
			return this;
		}

		long defaultTtlMs() {
			return this.defaultTtlMs;
		}

		CacheScope defaultCacheScope() {
			return this.defaultCacheScope;
		}

		public McpServer build() {
			Assert.notNull(this.serverInfo, "serverInfo must not be null");
			McpJsonMapper mapper = this.jsonMapper != null ? this.jsonMapper : McpJsonDefaults.getMapper();

			List<McpFeature> allFeatures = new ArrayList<>();
			if (this.toolsFeatureFactory != null) {
				allFeatures.add(this.toolsFeatureFactory.apply(mapper));
			}
			if (this.resourcesFeatureFactory != null) {
				allFeatures.add(this.resourcesFeatureFactory.apply(mapper));
			}
			if (this.promptsFeatureFactory != null) {
				allFeatures.add(this.promptsFeatureFactory.apply(mapper));
			}
			if (this.completionsFeatureFactory != null) {
				allFeatures.add(this.completionsFeatureFactory.apply(mapper));
			}
			allFeatures.addAll(this.features);

			ServerCapabilities.Builder capabilitiesBuilder = ServerCapabilities.builder();
			Set<String> inputRequiredMethods = new HashSet<>();
			McpRouter combined = McpRouter.empty();
			for (McpFeature feature : allFeatures) {
				combined = combined.and(feature.router());
				feature.capabilities(capabilitiesBuilder);
				inputRequiredMethods.addAll(feature.inputRequiredMethods());
			}

			// Subscriptions is wired up last: which change types it can honour depends
			// on the final set of registered primitives.
			SubscriptionsFeature subscriptionsFeature = null;
			if (this.changeFeed != null) {
				subscriptionsFeature = new SubscriptionsFeature(this.changeFeed, mapper, capabilitiesBuilder.hasTools(),
						capabilitiesBuilder.hasPrompts(), capabilitiesBuilder.hasResources());
				combined = combined.and(subscriptionsFeature.router());
				subscriptionsFeature.capabilities(capabilitiesBuilder);
			}

			ServerCapabilities capabilities = capabilitiesBuilder.build();

			DiscoverFeature discoverFeature = new DiscoverFeature(this.supportedVersions, capabilities,
					this.instructions, this.defaultTtlMs, this.defaultCacheScope);
			combined = discoverFeature.router().and(combined);

			RequestStateCodec codec = this.requestStateCodec != null ? this.requestStateCodec
					: HmacRequestStateCodec.builder().jsonMapper(mapper).build();

			return new McpServer(this.serverInfo, this.supportedVersions, mapper, combined, List.copyOf(this.filters),
					Set.copyOf(inputRequiredMethods), codec, subscriptionsFeature);
		}

	}

}
