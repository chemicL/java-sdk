/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.Optional;

import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.util.Assert;

/**
 * Wraps whatever a handler returns so that MRTR is opt-in: a handler that never needs to
 * ask the client for more input can return its plain result type through
 * {@link #complete(Result)} (or, more conveniently, never see this type at all - see the
 * {@code of(...)} factories on the handler interfaces), while a handler that does need
 * MRTR returns {@link #inputRequired(InputRequiredResult)}.
 * <p>
 * This type intentionally lives here, in server-side API, and not in
 * {@code modern.McpSchema}: which methods may answer with an {@link InputRequiredResult}
 * is a runtime rule enforced by {@code McpServer} and declared per-feature, not a
 * constraint baked into the wire types.
 *
 * @param <R> the handler's own complete-result type, e.g. {@code CallToolResult}
 * @author Dariusz Jędrzejczyk
 */
public final class McpOutcome<R extends Result> {

	private final Result result;

	private final R completeResult;

	private McpOutcome(Result result, R completeResult) {
		this.result = result;
		this.completeResult = completeResult;
	}

	/**
	 * The normal case: the request completed and {@code result} is the answer.
	 */
	public static <R extends Result> McpOutcome<R> complete(R result) {
		Assert.notNull(result, "result must not be null");
		return new McpOutcome<>(result, result);
	}

	/**
	 * The server needs more input before it can complete the original request.
	 */
	public static <R extends Result> McpOutcome<R> inputRequired(InputRequiredResult result) {
		Assert.notNull(result, "result must not be null");
		return new McpOutcome<>(result, null);
	}

	/**
	 * Escape hatch for results that are neither the handler's own complete-result type
	 * nor {@link InputRequiredResult}, e.g. a handle returned by an extension such as
	 * {@code io.modelcontextprotocol/tasks}.
	 */
	public static <R extends Result> McpOutcome<R> of(Result result) {
		Assert.notNull(result, "result must not be null");
		return new McpOutcome<>(result, null);
	}

	/** What actually goes on the wire. */
	public Result result() {
		return this.result;
	}

	public boolean isComplete() {
		return this.completeResult != null;
	}

	public Optional<R> completeResult() {
		return Optional.ofNullable(this.completeResult);
	}

}
