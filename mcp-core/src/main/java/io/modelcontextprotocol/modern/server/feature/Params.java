/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.Optional;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.modern.McpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

final class Params {

	private static final Logger logger = LoggerFactory.getLogger(Params.class);

	private Params() {
	}

	static <T> Optional<T> decode(McpJsonMapper jsonMapper, Object params, Class<T> type) {
		try {
			return Optional.of(jsonMapper.convertValue(params, type));
		}
		catch (RuntimeException ex) {
			logger.debug("Malformed params for {}", type.getSimpleName(), ex);
			return Optional.empty();
		}
	}

	static <O> Mono<O> malformed(Class<?> type) {
		return Mono.error(McpException.invalidParams("Malformed " + type.getSimpleName()));
	}

}
