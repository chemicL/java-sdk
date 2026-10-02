/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.util.Assert;

/**
 * The outcome of one round of a request: {@link Complete} with the handler's result, or
 * {@link InputRequired} when the server needs more input first. {@link #result()} is what
 * goes on the wire.
 *
 * @param <R> the handler's complete-result type, e.g. {@code CallToolResult}
 * @author Dariusz Jędrzejczyk
 */
public interface McpRoundResult<R extends Result> {

	/** What actually goes on the wire. */
	Result result();

	static <R extends Result> McpRoundResult<R> complete(R result) {
		return new Complete<>(result);
	}

	static <R extends Result> McpRoundResult<R> inputRequired(InputRequiredResult result) {
		return new InputRequired<>(result);
	}

	/** The request completed; {@code result} is the answer. */
	record Complete<R extends Result>(R result) implements McpRoundResult<R> {

		public Complete {
			Assert.notNull(result, "result must not be null");
		}

	}

	/** The server needs more input before it can complete the original request. */
	record InputRequired<R extends Result>(InputRequiredResult result) implements McpRoundResult<R> {

		public InputRequired {
			Assert.notNull(result, "result must not be null");
		}

	}

}
