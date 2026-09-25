/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.CallToolRequest;
import io.modelcontextprotocol.modern.McpSchema.CallToolResult;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * Handles one {@code tools/call}. Most implementations never need to see
 * {@link McpRoundResult}: use {@link #of} for a plain handler, or {@link #streaming} for
 * one that also pushes notifications. {@link #withInput}/ {@link #streamingWithInput} are
 * for handlers that may need MRTR.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface AsyncToolHandler {

	Mono<McpRoundResult<CallToolResult>> call(McpRequestContext ctx, CallToolRequest request);

	/** A handler that may push progress/log notifications before completing. */
	interface Streaming extends AsyncToolHandler {

		Mono<McpRoundResult<CallToolResult>> call(McpRequestContext ctx, CallToolRequest request,
				McpAsyncNotifier notifier);

		@Override
		default Mono<McpRoundResult<CallToolResult>> call(McpRequestContext ctx, CallToolRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static AsyncToolHandler of(BiFunction<McpRequestContext, CallToolRequest, Mono<CallToolResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> fn.apply(ctx, request).map(McpRoundResult::complete);
	}

	static AsyncToolHandler withInput(
			BiFunction<McpRequestContext, CallToolRequest, Mono<McpRoundResult<CallToolResult>>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(AsyncStreamingFunction<CallToolRequest, CallToolResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> fn.apply(ctx, request, notifier).map(McpRoundResult::complete);
	}

	static Streaming streamingWithInput(AsyncStreamingFunction<CallToolRequest, McpRoundResult<CallToolResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
