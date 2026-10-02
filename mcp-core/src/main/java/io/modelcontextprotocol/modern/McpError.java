/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern;

import io.modelcontextprotocol.modern.JsonRpc.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.util.Assert;

/**
 * An exception that is reported to the peer as the given JSON-RPC error.
 *
 * @author Dariusz Jędrzejczyk
 */
public class McpError extends RuntimeException {

	private final JSONRPCError jsonRpcError;

	public McpError(JSONRPCError jsonRpcError) {
		super(jsonRpcError.message());
		this.jsonRpcError = jsonRpcError;
	}

	public JSONRPCError getJsonRpcError() {
		return this.jsonRpcError;
	}

	@Override
	public String toString() {
		return super.toString() + "\n" + this.jsonRpcError;
	}

	public static Builder builder(int errorCode) {
		return new Builder(errorCode);
	}

	public static final class Builder {

		private final int code;

		private String message;

		private Object data;

		private Builder(int code) {
			this.code = code;
		}

		public Builder message(String message) {
			this.message = message;
			return this;
		}

		public Builder data(Object data) {
			this.data = data;
			return this;
		}

		public McpError build() {
			Assert.hasText(message, "message must not be empty");
			return new McpError(new JSONRPCError(this.code, this.message, this.data));
		}

	}

}
