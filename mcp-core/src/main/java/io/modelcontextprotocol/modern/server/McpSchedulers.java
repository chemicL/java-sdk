/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

/**
 * Replaces the legacy {@code immediateExecution} flag: the transport, not a global
 * setting, decides which scheduler a sync handler runs on, by writing it into the Reactor
 * context before subscribing. Only the transport knows whether its calling thread belongs
 * to the request (a servlet container thread does; a single-threaded stdio reader does
 * not).
 *
 * @author Dariusz Jędrzejczyk
 */
public final class McpSchedulers {

	/** Context key for the scheduler unary sync handlers run on. */
	public static final String HANDLER_SCHEDULER_KEY = McpSchedulers.class.getName() + ".handler";

	/**
	 * Context key for the scheduler sync streaming handlers and their notifiers run on.
	 */
	public static final String STREAMING_SCHEDULER_KEY = McpSchedulers.class.getName() + ".streaming";

	private McpSchedulers() {
	}

	public static Scheduler handlerScheduler(ContextView contextView) {
		return safeguard(contextView.getOrDefault(HANDLER_SCHEDULER_KEY, Schedulers.boundedElastic()));
	}

	public static Scheduler streamingScheduler(ContextView contextView) {
		return safeguard(contextView.getOrDefault(STREAMING_SCHEDULER_KEY, Schedulers.boundedElastic()));
	}

	/**
	 * Guard against a transport mistakenly asking to run on the calling thread while that
	 * thread is a non-blocking Reactor thread; blocking one of those would starve the
	 * event loop.
	 */
	private static Scheduler safeguard(Scheduler scheduler) {
		if (scheduler == Schedulers.immediate() && Schedulers.isInNonBlockingThread()) {
			return Schedulers.boundedElastic();
		}
		return scheduler;
	}

}
