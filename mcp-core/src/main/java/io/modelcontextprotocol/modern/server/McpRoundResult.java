/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import io.modelcontextprotocol.modern.McpSchema.InputRequiredResult;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.util.Assert;

/**
 * What a handler produces for one round of a request: either the request is done
 * ({@link Complete}, wrapping the handler's own result type), or the server needs more
 * input before it can finish ({@link InputRequired}). Both are a {@link Result}, so
 * {@link #result()} is what actually goes on the wire regardless of which one a handler
 * returned.
 * <p>
 * For {@code tools/call}, {@code resources/read} and {@code prompts/get} these two are
 * the whole story - not sealed, though: which methods may answer with
 * {@link InputRequired} is a runtime rule enforced by {@code McpServer} and declared
 * per-feature, not a constraint baked into this type, and an extension with its own round
 * semantics can implement this interface directly instead of using either record.
 *
 * @param <R> the handler's own complete-result type, e.g. {@code CallToolResult}
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
