/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCMessage;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCNotification;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCRequest;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse;
import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.modern.McpError;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.CacheScope;
import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities.Elicitation;
import io.modelcontextprotocol.modern.McpSchema.ElicitUrlRequest;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.Implementation;
import io.modelcontextprotocol.modern.McpSchema.InputRequest;
import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.LoggingLevel;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.MissingRequiredClientCapabilityData;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.McpSchema.UnsupportedProtocolVersionData;
import io.modelcontextprotocol.modern.server.feature.McpChangeFeed;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * A stateless, immutable dispatcher that routes each request by method to the
 * {@link McpFeature} serving it. It validates {@code _meta}, negotiates the version,
 * answers {@code server/discover}, stamps {@code serverInfo}, maps errors and seals MRTR
 * {@code requestState}.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class McpServer implements McpRequestManager {

	private static final Logger logger = LoggerFactory.getLogger(McpServer.class);

	private static final TypeRef<Map<String, Object>> MAP_TYPE_REF = new TypeRef<>() {
	};

	private final Implementation serverInfo;

	private final List<String> supportedVersions;

	private final McpJsonMapper jsonMapper;

	private final Map<String, McpFeature> routes;

	private final Set<String> inputRequiredMethods;

	private final RequestStateCodec requestStateCodec;

	private final SubscriptionsFeature subscriptionsFeature;

	private static final Set<String> MRTR_ELIGIBLE_METHODS = Set.of(McpSchema.METHOD_TOOLS_CALL,
			McpSchema.METHOD_RESOURCES_READ, McpSchema.METHOD_PROMPTS_GET);

	private McpServer(Implementation serverInfo, List<String> supportedVersions, McpJsonMapper jsonMapper,
			Map<String, McpFeature> routes, Set<String> inputRequiredMethods, RequestStateCodec requestStateCodec,
			SubscriptionsFeature subscriptionsFeature) {
		this.serverInfo = serverInfo;
		this.supportedVersions = supportedVersions;
		this.jsonMapper = jsonMapper;
		this.routes = routes;
		this.inputRequiredMethods = inputRequiredMethods;
		this.requestStateCodec = requestStateCodec;
		this.subscriptionsFeature = subscriptionsFeature;
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
				return Mono.just(McpInvocation.single(Mono.just(errorResponse(id, ex))));
			}
		});
	}

	private Mono<McpInvocation> doResolve(McpTransportContext transportContext, JSONRPCRequest request, Object id,
			boolean blocking) {
		Map<String, Object> paramsMap = toParamsMap(request.params());
		if (paramsMap == null) {
			return singleError(id, ErrorCodes.INVALID_PARAMS, "params is required");
		}
		Map<String, Object> meta = asMetaMap(paramsMap.get("_meta"));
		if (meta == null) {
			return singleError(id, ErrorCodes.INVALID_PARAMS, "params._meta is required");
		}
		Object versionRaw = meta.get(MetaKeys.PROTOCOL_VERSION);
		if (!(versionRaw instanceof String protocolVersion) || protocolVersion.isBlank()) {
			return singleError(id, ErrorCodes.INVALID_PARAMS, "_meta['" + MetaKeys.PROTOCOL_VERSION + "'] is required");
		}
		Object capabilitiesRaw = meta.get(MetaKeys.CLIENT_CAPABILITIES);
		if (capabilitiesRaw == null) {
			return singleError(id, ErrorCodes.INVALID_PARAMS,
					"_meta['" + MetaKeys.CLIENT_CAPABILITIES + "'] is required");
		}
		if (!this.supportedVersions.contains(protocolVersion)) {
			return singleError(id, ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION, "Unsupported protocol version",
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
				return singleError(id, ErrorCodes.INVALID_PARAMS, "_meta['" + MetaKeys.LOG_LEVEL + "'] is invalid");
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
			if (requestStateRaw != null) {
				// Anything but a sealed string would reach the handler unverified.
				if (!(requestStateRaw instanceof String sealed)) {
					return singleError(id, ErrorCodes.INVALID_PARAMS, "requestState must be a string");
				}
				effectiveParams = new LinkedHashMap<>(paramsMap);
				effectiveParams.put("requestState", this.requestStateCodec.open(ctx, sealed));
			}
		}
		Object finalParams = effectiveParams;

		McpFeature feature = this.routes.get(request.method());
		if (feature == null) {
			return singleError(id, ErrorCodes.METHOD_NOT_FOUND, "Method not found: " + request.method());
		}
		return feature.resolve(ctx)
			.map(handler -> dispatch(ctx, handler, finalParams))
			.switchIfEmpty(Mono.fromSupplier(() -> singleErrorInvocation(id, ErrorCodes.METHOD_NOT_FOUND,
					"Method not found: " + request.method(), null)))
			.onErrorResume(err -> Mono.just(singleErrorInvocation(id, err)));
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
		return McpInvocation.single(response);
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

	private Mono<McpInvocation> singleError(Object id, int code, String message) {
		return singleError(id, code, message, null);
	}

	private Mono<McpInvocation> singleError(Object id, int code, String message, Object data) {
		return Mono.just(singleErrorInvocation(id, code, message, data));
	}

	private McpInvocation singleErrorInvocation(Object id, int code, String message, Object data) {
		return McpInvocation.single(Mono.just(JSONRPCResponse.error(id, new JSONRPCError(code, message, data))));
	}

	private McpInvocation singleErrorInvocation(Object id, Throwable throwable) {
		return McpInvocation.single(Mono.just(errorResponse(id, throwable)));
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
	 * Builds an immutable {@link McpServer} from registered features.
	 */
	public static final class Builder {

		private Implementation serverInfo;

		private String instructions;

		private List<String> supportedVersions = List.of(McpSchema.LATEST_PROTOCOL_VERSION);

		private McpJsonMapper jsonMapper;

		private final List<McpFeature> features = new ArrayList<>();

		private long discoverTtlMs = 0L;

		private CacheScope discoverCacheScope = CacheScope.PRIVATE;

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

		public Builder features(List<? extends McpFeature> features) {
			Assert.notNull(features, "features must not be null");
			features.forEach(this::feature);
			return this;
		}

		/**
		 * Registers {@code subscriptions/listen}, backed by {@code feed}, for the tools,
		 * prompts and resources features that are registered.
		 */
		public Builder subscriptions(McpChangeFeed feed) {
			Assert.notNull(feed, "feed must not be null");
			this.changeFeed = feed;
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
		 * Caching hints for the {@code server/discover} result. Defaults to no caching.
		 */
		public Builder discoverCache(long ttlMs, CacheScope cacheScope) {
			Assert.notNull(cacheScope, "cacheScope must not be null");
			this.discoverTtlMs = ttlMs;
			this.discoverCacheScope = cacheScope;
			return this;
		}

		public McpServer build() {
			Assert.notNull(this.serverInfo, "serverInfo must not be null");
			McpJsonMapper mapper = this.jsonMapper != null ? this.jsonMapper : McpJsonDefaults.getMapper();

			List<McpFeature> allFeatures = new ArrayList<>(this.features);

			ServerCapabilities.Builder capabilitiesBuilder = ServerCapabilities.builder();
			for (McpFeature feature : allFeatures) {
				feature.capabilities(capabilitiesBuilder);
			}

			// Subscriptions and discover are wired up last: which change types
			// subscriptions can honour depends on the registered primitives, and discover
			// advertises the final capabilities.
			SubscriptionsFeature subscriptionsFeature = null;
			if (this.changeFeed != null) {
				subscriptionsFeature = new SubscriptionsFeature(this.changeFeed, mapper, capabilitiesBuilder.hasTools(),
						capabilitiesBuilder.hasPrompts(), capabilitiesBuilder.hasResources());
				subscriptionsFeature.capabilities(capabilitiesBuilder);
				allFeatures.add(subscriptionsFeature);
			}
			allFeatures.add(new DiscoverFeature(this.supportedVersions, capabilitiesBuilder.build(), this.instructions,
					this.discoverTtlMs, this.discoverCacheScope));

			Map<String, McpFeature> routes = new HashMap<>();
			Set<String> inputRequiredMethods = new HashSet<>();
			for (McpFeature feature : allFeatures) {
				for (String method : feature.methods()) {
					McpFeature existing = routes.putIfAbsent(method, feature);
					if (existing != null) {
						throw new IllegalStateException("Method '" + method + "' is served by both "
								+ existing.getClass().getName() + " and " + feature.getClass().getName());
					}
				}
				inputRequiredMethods.addAll(feature.inputRequiredMethods());
			}

			RequestStateCodec codec = this.requestStateCodec != null ? this.requestStateCodec
					: HmacRequestStateCodec.builder().jsonMapper(mapper).build();

			return new McpServer(this.serverInfo, this.supportedVersions, mapper, Map.copyOf(routes),
					Set.copyOf(inputRequiredMethods), codec, subscriptionsFeature);
		}

	}

}
