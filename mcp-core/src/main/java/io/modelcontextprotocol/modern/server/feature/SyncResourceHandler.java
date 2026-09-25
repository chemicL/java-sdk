/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.util.Assert;

/**
 * The blocking counterpart of {@link AsyncResourceHandler}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface SyncResourceHandler {

	McpRoundResult<ReadResourceResult> read(McpRequestContext ctx, ReadResourceRequest request);

	interface Streaming extends SyncResourceHandler {

		McpRoundResult<ReadResourceResult> read(McpRequestContext ctx, ReadResourceRequest request,
				McpSyncNotifier notifier);

		@Override
		default McpRoundResult<ReadResourceResult> read(McpRequestContext ctx, ReadResourceRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static SyncResourceHandler of(BiFunction<McpRequestContext, ReadResourceRequest, ReadResourceResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> McpRoundResult.complete(fn.apply(ctx, request));
	}

	static SyncResourceHandler withInput(
			BiFunction<McpRequestContext, ReadResourceRequest, McpRoundResult<ReadResourceResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(SyncStreamingFunction<ReadResourceRequest, ReadResourceResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> McpRoundResult.complete(fn.apply(ctx, request, notifier));
	}

	static Streaming streamingWithInput(
			SyncStreamingFunction<ReadResourceRequest, McpRoundResult<ReadResourceResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
