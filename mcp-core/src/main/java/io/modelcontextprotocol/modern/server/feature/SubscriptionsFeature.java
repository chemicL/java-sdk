/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.McpSchema.ListChangedParams;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.McpSchema.ResourceUpdatedParams;
import io.modelcontextprotocol.modern.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.modern.McpSchema.SubscriptionFilter;
import io.modelcontextprotocol.modern.McpSchema.SubscriptionsAcknowledgedParams;
import io.modelcontextprotocol.modern.McpSchema.SubscriptionsListenRequest;
import io.modelcontextprotocol.modern.McpSchema.SubscriptionsListenResult;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpFeature;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRouter;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * The {@code subscriptions/listen} feature: a long-lived {@link McpHandler.Streaming}
 * that first acknowledges the honoured subset of the requested filter, then forwards
 * matching changes from a {@link McpChangeFeed}, tagging every message with the listen
 * request's id as {@code _meta.subscriptionId}.
 * <p>
 * Must be added after every other feature: which types it can honour depends on whether
 * tools/prompts/resources are actually registered.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class SubscriptionsFeature implements McpFeature {

	private final McpChangeFeed feed;

	private final McpJsonMapper jsonMapper;

	private final boolean hasTools;

	private final boolean hasPrompts;

	private final boolean hasResources;

	private final Sinks.Empty<Void> shutdown = Sinks.empty();

	public SubscriptionsFeature(McpChangeFeed feed, McpJsonMapper jsonMapper, boolean hasTools, boolean hasPrompts,
			boolean hasResources) {
		Assert.notNull(feed, "feed must not be null");
		this.feed = feed;
		this.jsonMapper = jsonMapper;
		this.hasTools = hasTools;
		this.hasPrompts = hasPrompts;
		this.hasResources = hasResources;
	}

	/** Ends every active listen stream with a graceful {@code complete} result. */
	public void closeGracefully() {
		this.shutdown.tryEmitEmpty();
	}

	@Override
	public McpRouter router() {
		return ctx -> {
			if (!McpSchema.METHOD_SUBSCRIPTIONS_LISTEN.equals(ctx.method())) {
				return Mono.empty();
			}
			return Mono.just((McpHandler.Streaming) this::listen);
		};
	}

	@Override
	public void capabilities(ServerCapabilities.Builder builder) {
		if (this.hasTools) {
			builder.toolsListChanged(true);
		}
		if (this.hasPrompts) {
			builder.promptsListChanged(true);
		}
		if (this.hasResources) {
			builder.resourcesSubscribe(this.feed.supportsResourceUpdates(), true);
		}
	}

	private Mono<Result> listen(McpRequestContext ctx, Object params, McpAsyncNotifier notifier) {
		SubscriptionsListenRequest request = params == null ? null
				: this.jsonMapper.convertValue(params, SubscriptionsListenRequest.class);
		SubscriptionFilter requested = request == null || request.notifications() == null ? SubscriptionFilter.EMPTY
				: request.notifications();
		SubscriptionFilter honoured = intersect(requested);

		Map<String, Object> subscriptionMeta = Map.of(MetaKeys.SUBSCRIPTION_ID, ctx.requestId());
		Mono<Void> ack = notifier.notify(McpSchema.METHOD_NOTIFICATION_SUBSCRIPTIONS_ACKNOWLEDGED,
				new SubscriptionsAcknowledgedParams(honoured, subscriptionMeta));

		Flux<Void> forwardChanges = this.feed.changes()
			.filter(change -> matches(change, honoured))
			.takeUntilOther(this.shutdown.asMono())
			.concatMap(change -> notifier.notify(methodFor(change), paramsFor(change, subscriptionMeta)));

		return ack.thenMany(forwardChanges)
			.then(Mono.fromSupplier(() -> (Result) SubscriptionsListenResult.forSubscription(ctx.requestId())));
	}

	private SubscriptionFilter intersect(SubscriptionFilter requested) {
		Boolean tools = (this.hasTools && requested.wantsToolsListChanged()) ? Boolean.TRUE : null;
		Boolean prompts = (this.hasPrompts && requested.wantsPromptsListChanged()) ? Boolean.TRUE : null;
		Boolean resources = (this.hasResources && requested.wantsResourcesListChanged()) ? Boolean.TRUE : null;
		List<String> resourceSubs = (this.hasResources && this.feed.supportsResourceUpdates())
				? requested.resourceSubscriptionsOrEmpty() : List.of();
		return new SubscriptionFilter(tools, prompts, resources, resourceSubs.isEmpty() ? null : resourceSubs);
	}

	private static boolean matches(ServerChange change, SubscriptionFilter honoured) {
		if (change instanceof ServerChange.ToolsListChanged) {
			return honoured.wantsToolsListChanged();
		}
		if (change instanceof ServerChange.PromptsListChanged) {
			return honoured.wantsPromptsListChanged();
		}
		if (change instanceof ServerChange.ResourcesListChanged) {
			return honoured.wantsResourcesListChanged();
		}
		if (change instanceof ServerChange.ResourceUpdated updated) {
			return honoured.resourceSubscriptionsOrEmpty().contains(updated.uri());
		}
		return false;
	}

	private static String methodFor(ServerChange change) {
		if (change instanceof ServerChange.ToolsListChanged) {
			return McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED;
		}
		if (change instanceof ServerChange.PromptsListChanged) {
			return McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED;
		}
		if (change instanceof ServerChange.ResourcesListChanged) {
			return McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED;
		}
		if (change instanceof ServerChange.ResourceUpdated) {
			return McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED;
		}
		throw new IllegalStateException("Unrecognized change: " + change);
	}

	private static Object paramsFor(ServerChange change, Map<String, Object> subscriptionMeta) {
		if (change instanceof ServerChange.ResourceUpdated updated) {
			return new ResourceUpdatedParams(updated.uri(), subscriptionMeta);
		}
		return new ListChangedParams(subscriptionMeta);
	}

}
