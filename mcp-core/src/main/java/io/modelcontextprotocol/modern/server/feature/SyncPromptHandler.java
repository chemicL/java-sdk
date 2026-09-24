/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.McpSchema.GetPromptResult;
import io.modelcontextprotocol.modern.server.McpOutcome;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.util.Assert;

/**
 * The blocking counterpart of {@link AsyncPromptHandler}.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface SyncPromptHandler {

	McpOutcome<GetPromptResult> get(McpRequestContext ctx, GetPromptRequest request);

	interface Streaming extends SyncPromptHandler {

		McpOutcome<GetPromptResult> get(McpRequestContext ctx, GetPromptRequest request, McpSyncNotifier notifier);

		@Override
		default McpOutcome<GetPromptResult> get(McpRequestContext ctx, GetPromptRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static SyncPromptHandler of(BiFunction<McpRequestContext, GetPromptRequest, GetPromptResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> McpOutcome.complete(fn.apply(ctx, request));
	}

	static SyncPromptHandler withInput(
			BiFunction<McpRequestContext, GetPromptRequest, McpOutcome<GetPromptResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(SyncStreamingFunction<GetPromptRequest, GetPromptResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> McpOutcome.complete(fn.apply(ctx, request, notifier));
	}

	static Streaming streamingWithInput(SyncStreamingFunction<GetPromptRequest, McpOutcome<GetPromptResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
