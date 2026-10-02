/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.concurrent.Callable;

import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.spec.McpSchema.LoggingLevel;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Bridges sync repositories and handlers onto Reactor. Where the sync code runs is
 * decided per request by {@link McpRequestContext#isBlocking()}: on the calling thread
 * for a blocking caller, on {@code boundedElastic} otherwise.
 *
 * @author Dariusz Jędrzejczyk
 */
final class SyncAdapters {

	private SyncAdapters() {
	}

	static <T> Mono<T> unary(McpRequestContext ctx, Callable<T> call) {
		return onCallerOrOffload(ctx, Mono.fromCallable(call));
	}

	static <RES> Mono<RES> streaming(McpRequestContext ctx, McpAsyncNotifier asyncNotifier, SyncCall<RES> call) {
		McpSyncNotifier syncNotifier = new BlockingSyncNotifier(asyncNotifier);
		return onCallerOrOffload(ctx, Mono.fromCallable(() -> call.call(syncNotifier)));
	}

	private static <T> Mono<T> onCallerOrOffload(McpRequestContext ctx, Mono<T> mono) {
		return ctx.isBlocking() ? mono : mono.subscribeOn(Schedulers.boundedElastic());
	}

	@FunctionalInterface
	interface SyncCall<RES> {

		RES call(McpSyncNotifier notifier);

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
