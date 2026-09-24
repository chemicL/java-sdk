/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.server.McpOutcome;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.util.Assert;

/**
 * The blocking counterpart of {@link AsyncToolHandler}. No Reactor types appear in this
 * interface.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface SyncToolHandler {

	McpOutcome<CallToolResult> call(McpRequestContext ctx, CallToolRequest request);

	interface Streaming extends SyncToolHandler {

		McpOutcome<CallToolResult> call(McpRequestContext ctx, CallToolRequest request, McpSyncNotifier notifier);

		@Override
		default McpOutcome<CallToolResult> call(McpRequestContext ctx, CallToolRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static SyncToolHandler of(BiFunction<McpRequestContext, CallToolRequest, CallToolResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> McpOutcome.complete(fn.apply(ctx, request));
	}

	static SyncToolHandler withInput(BiFunction<McpRequestContext, CallToolRequest, McpOutcome<CallToolResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(SyncStreamingFunction<CallToolRequest, CallToolResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> McpOutcome.complete(fn.apply(ctx, request, notifier));
	}

	static Streaming streamingWithInput(SyncStreamingFunction<CallToolRequest, McpOutcome<CallToolResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
