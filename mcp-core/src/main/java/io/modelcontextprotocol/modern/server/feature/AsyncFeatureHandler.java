/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.function.BiFunction;

import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * Handles one request for a resolved primitive ({@code tools/call}, {@code prompts/get},
 * {@code resources/read}). Most implementations never need to see {@link McpRoundResult}:
 * use {@link #of} for a single handler, or {@link #streaming} for one that also pushes
 * notifications. {@link #withInput}/ {@link #streamingWithInput} are for handlers that
 * may need MRTR.
 *
 * @param <REQ> the typed request
 * @param <RES> the typed result
 * @author Dariusz Jędrzejczyk
 */
public interface AsyncFeatureHandler<REQ, RES extends Result> {

	Mono<McpRoundResult<RES>> handle(McpRequestContext ctx, REQ request);

	/** A handler that may push progress/log notifications before completing. */
	interface Streaming<REQ, RES extends Result> extends AsyncFeatureHandler<REQ, RES> {

		Mono<McpRoundResult<RES>> handle(McpRequestContext ctx, REQ request, McpAsyncNotifier notifier);

		@Override
		default Mono<McpRoundResult<RES>> handle(McpRequestContext ctx, REQ request) {
			throw new UnsupportedOperationException("Streaming handlers must be invoked with a notifier");
		}

	}

	static <REQ, RES extends Result> AsyncFeatureHandler<REQ, RES> of(
			BiFunction<McpRequestContext, REQ, Mono<RES>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request) -> fn.apply(ctx, request).map(McpRoundResult::complete);
	}

	static <REQ, RES extends Result> AsyncFeatureHandler<REQ, RES> withInput(
			BiFunction<McpRequestContext, REQ, Mono<McpRoundResult<RES>>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

	static <REQ, RES extends Result> Streaming<REQ, RES> streaming(AsyncStreamingFunction<REQ, RES> fn) {
		Assert.notNull(fn, "fn must not be null");
		return (ctx, request, notifier) -> fn.apply(ctx, request, notifier).map(McpRoundResult::complete);
	}

	static <REQ, RES extends Result> Streaming<REQ, RES> streamingWithInput(
			AsyncStreamingFunction<REQ, McpRoundResult<RES>> fn) {
		Assert.notNull(fn, "fn must not be null");
		return fn::apply;
	}

}
