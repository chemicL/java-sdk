/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpError;
import io.modelcontextprotocol.modern.McpSchema.ErrorCodes;
import io.modelcontextprotocol.modern.McpSchema.Result;
import io.modelcontextprotocol.modern.server.McpHandler;
import io.modelcontextprotocol.modern.server.McpRoundResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FeatureHandlers {

	private static final Logger logger = LoggerFactory.getLogger(FeatureHandlers.class);

	private FeatureHandlers() {
	}

	static <REQ, RES extends Result> McpHandler toMcpHandler(AsyncFeatureHandler<REQ, RES> handler,
			Class<REQ> requestType, McpJsonMapper jsonMapper) {
		if (handler instanceof AsyncFeatureHandler.Streaming<REQ, RES> streaming) {
			return (McpHandler.Streaming) (ctx, params, notifier) -> {
				REQ request = convertParams(jsonMapper, params, requestType);
				return streaming.handle(ctx, request, notifier).map(McpRoundResult::result);
			};
		}
		return (ctx, params) -> {
			REQ request = convertParams(jsonMapper, params, requestType);
			return handler.handle(ctx, request).map(McpRoundResult::result);
		};
	}

	static <T> T convertParams(McpJsonMapper jsonMapper, Object params, Class<T> type) {
		try {
			return jsonMapper.convertValue(params, type);
		}
		catch (RuntimeException ex) {
			// The mapper's message may expose internals; the client only learns which
			// request type it got wrong.
			logger.debug("Malformed params for {}", type.getSimpleName(), ex);
			throw McpError.builder(ErrorCodes.INVALID_PARAMS).message("Malformed " + type.getSimpleName()).build();
		}
	}

}
