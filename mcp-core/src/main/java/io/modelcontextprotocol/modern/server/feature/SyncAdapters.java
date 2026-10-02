/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.concurrent.Callable;

import io.modelcontextprotocol.modern.McpSchema.LoggingLevel;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Converts sync (blocking) repository calls and handlers to their async (Reactor)
 * equivalents. Only the programming paradigm changes: a single handler stays single and a
 * streaming handler stays streaming. Where the sync code runs is decided per request by
 * {@link McpRequestContext#isBlocking()}: on the calling thread for a blocking caller, on
 * {@code boundedElastic} otherwise.
 *
 * @author Dariusz Jędrzejczyk
 */
final class SyncAdapters {

	private SyncAdapters() {
	}

	/**
	 * Runs one blocking call as a {@link Mono}; a {@code null} result completes empty.
	 */
	static <T> Mono<T> toAsync(McpRequestContext ctx, Callable<T> call) {
		return onCallerOrOffload(ctx, Mono.fromCallable(call));
	}

	static <REQ, RES extends Result> AsyncFeatureHandler<REQ, RES> toAsync(SyncFeatureHandler<REQ, RES> handler) {
		if (handler instanceof SyncFeatureHandler.Streaming<REQ, RES> streaming) {
			return (AsyncFeatureHandler.Streaming<REQ, RES>) (ctx, request, notifier) -> {
				McpSyncNotifier syncNotifier = new BlockingSyncNotifier(notifier);
				return toAsync(ctx, () -> streaming.handle(ctx, request, syncNotifier));
			};
		}
		return (AsyncFeatureHandler<REQ, RES>) (ctx, request) -> toAsync(ctx, () -> handler.handle(ctx, request));
	}

	private static <T> Mono<T> onCallerOrOffload(McpRequestContext ctx, Mono<T> mono) {
		return ctx.isBlocking() ? mono : mono.subscribeOn(Schedulers.boundedElastic());
	}

	/**
	 * Blocks on the async notifier's Monos, which complete synchronously once the
	 * notification is handed to the stream. Only ever invoked from sync handler code,
	 * which by construction runs on a thread allowed to block.
	 */
	private static final class BlockingSyncNotifier implements McpSyncNotifier {

		private final McpAsyncNotifier delegate;

		private BlockingSyncNotifier(McpAsyncNotifier delegate) {
			this.delegate = delegate;
		}

		@Override
		public void progress(double progress, Double total, String message) {
			this.delegate.progress(progress, total, message).block();
		}

		@Override
		public void log(LoggingLevel level, String logger, Object data) {
			this.delegate.log(level, logger, data).block();
		}

		@Override
		public void notify(String method, Object params) {
			this.delegate.notify(method, params).block();
		}

	}

}
