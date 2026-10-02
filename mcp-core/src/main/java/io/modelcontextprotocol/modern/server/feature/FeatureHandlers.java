/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRoundResult;

final class FeatureHandlers {

	private FeatureHandlers() {
	}

	static <REQ, RES extends Result> McpHandler toMcpHandler(AsyncFeatureHandler<REQ, RES> handler,
			Class<REQ> requestType, McpJsonMapper jsonMapper) {
		if (handler instanceof AsyncFeatureHandler.Streaming<REQ, RES> streaming) {
			return (McpHandler.Streaming) (ctx, params, notifier) -> {
				REQ request = jsonMapper.convertValue(params, requestType);
				return streaming.handle(ctx, request, notifier).map(McpRoundResult::result);
			};
		}
		return (ctx, params) -> {
			REQ request = jsonMapper.convertValue(params, requestType);
			return handler.handle(ctx, request).map(McpRoundResult::result);
		};
	}

}
