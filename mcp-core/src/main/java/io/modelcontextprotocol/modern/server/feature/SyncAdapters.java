/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.concurrent.Callable;

import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpSchedulers;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.spec.McpSchema.LoggingLevel;
import reactor.core.publisher.Mono;

/**
 * Bridges sync repositories and handlers onto Reactor, running on whichever scheduler the
 * transport wrote into the context (see {@link McpSchedulers}) - never on a flag the SDK
 * itself decides.
 *
 * @author Dariusz Jędrzejczyk
 */
final class SyncAdapters {

	private SyncAdapters() {
	}

	static <T> Mono<T> unary(Callable<T> call) {
		return Mono.deferContextual(
				contextView -> Mono.fromCallable(call).subscribeOn(McpSchedulers.handlerScheduler(contextView)));
	}

	static <RES> Mono<RES> streaming(McpAsyncNotifier asyncNotifier, SyncCall<RES> call) {
		return Mono.deferContextual(contextView -> {
			var scheduler = McpSchedulers.streamingScheduler(contextView);
			McpSyncNotifier syncNotifier = new BlockingSyncNotifier(asyncNotifier);
			return Mono.fromCallable(() -> call.call(syncNotifier)).subscribeOn(scheduler);
		});
	}

	@FunctionalInterface
	interface SyncCall<RES> {

		RES call(McpSyncNotifier notifier);

	}

	/**
	 * Blocks on the async notifier's (synchronous, non-blocking-in-practice) Monos. Safe
	 * here because this is only ever invoked from inside the {@code Mono.fromCallable}
	 * above, already running on a blocking-capable scheduler.
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
