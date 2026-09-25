/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.modern.McpSchema.GetPromptResult;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * Handles one {@code prompts/get}. See {@link AsyncToolHandler} for the factory
 * conventions.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface AsyncPromptHandler {

	Mono<McpRoundResult<GetPromptResult>> get(McpRequestContext ctx, GetPromptRequest request);

	interface Streaming extends AsyncPromptHandler {

		Mono<McpRoundResult<GetPromptResult>> get(McpRequestContext ctx, GetPromptRequest request,
				McpAsyncNotifier notifier);

		@Override
		default Mono<McpRoundResult<GetPromptResult>> get(McpRequestContext ctx, GetPromptRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static AsyncPromptHandler of(BiFunction<McpRequestContext, GetPromptRequest, Mono<GetPromptResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> fn.apply(ctx, request).map(McpRoundResult::complete);
	}

	static AsyncPromptHandler withInput(
			BiFunction<McpRequestContext, GetPromptRequest, Mono<McpRoundResult<GetPromptResult>>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(AsyncStreamingFunction<GetPromptRequest, GetPromptResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> fn.apply(ctx, request, notifier).map(McpRoundResult::complete);
	}

	static Streaming streamingWithInput(AsyncStreamingFunction<GetPromptRequest, McpRoundResult<GetPromptResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
