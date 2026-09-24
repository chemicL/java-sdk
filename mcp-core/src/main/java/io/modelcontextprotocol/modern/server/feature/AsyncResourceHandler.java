/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.modern.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpOutcome;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * Handles one {@code resources/read}. See {@link AsyncToolHandler} for the factory
 * conventions.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface AsyncResourceHandler {

	Mono<McpOutcome<ReadResourceResult>> read(McpRequestContext ctx, ReadResourceRequest request);

	interface Streaming extends AsyncResourceHandler {

		Mono<McpOutcome<ReadResourceResult>> read(McpRequestContext ctx, ReadResourceRequest request,
				McpAsyncNotifier notifier);

		@Override
		default Mono<McpOutcome<ReadResourceResult>> read(McpRequestContext ctx, ReadResourceRequest request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static AsyncResourceHandler of(BiFunction<McpRequestContext, ReadResourceRequest, Mono<ReadResourceResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> fn.apply(ctx, request).map(McpOutcome::complete);
	}

	static AsyncResourceHandler withInput(
			BiFunction<McpRequestContext, ReadResourceRequest, Mono<McpOutcome<ReadResourceResult>>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static Streaming streaming(AsyncStreamingFunction<ReadResourceRequest, ReadResourceResult> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> fn.apply(ctx, request, notifier).map(McpOutcome::complete);
	}

	static Streaming streamingWithInput(
			AsyncStreamingFunction<ReadResourceRequest, McpOutcome<ReadResourceResult>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
