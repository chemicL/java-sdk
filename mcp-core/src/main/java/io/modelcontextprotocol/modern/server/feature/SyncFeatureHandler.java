/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.util.Assert;

/**
 * The blocking counterpart of {@link AsyncFeatureHandler}. No Reactor types appear in
 * this interface.
 *
 * @param <REQ> the typed request
 * @param <RES> the typed result
 * @author Dariusz Jędrzejczyk
 */
public interface SyncFeatureHandler<REQ, RES extends Result> {

	McpRoundResult<RES> handle(McpRequestContext ctx, REQ request);

	interface Streaming<REQ, RES extends Result> extends SyncFeatureHandler<REQ, RES> {

		McpRoundResult<RES> handle(McpRequestContext ctx, REQ request, McpSyncNotifier notifier);

		@Override
		default McpRoundResult<RES> handle(McpRequestContext ctx, REQ request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static <REQ, RES extends Result> SyncFeatureHandler<REQ, RES> of(BiFunction<McpRequestContext, REQ, RES> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> McpRoundResult.complete(fn.apply(ctx, request));
	}

	static <REQ, RES extends Result> SyncFeatureHandler<REQ, RES> withInput(
			BiFunction<McpRequestContext, REQ, McpRoundResult<RES>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static <REQ, RES extends Result> Streaming<REQ, RES> streaming(SyncStreamingFunction<REQ, RES> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> McpRoundResult.complete(fn.apply(ctx, request, notifier));
	}

	static <REQ, RES extends Result> Streaming<REQ, RES> streamingWithInput(
			SyncStreamingFunction<REQ, McpRoundResult<RES>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
