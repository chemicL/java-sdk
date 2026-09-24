/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

import java.util.Map;

import io.modelcontextprotocol.json.McpJsonMapper;

/**
 * Reads a typed value out of the raw {@code inputResponses} map a retried
 * {@code tools/call}, {@code resources/read} or {@code prompts/get} carries, keyed by the
 * same server-assigned key the original {@code InputRequiredResult} used.
 *
 * @author Dariusz Jędrzejczyk
 */
public final class InputResponses {

	private InputResponses() {
	}

	/**
	 * @return the response for {@code key} converted to {@code type}, or {@code null} if
	 * {@code inputResponses} is {@code null} or has no entry for {@code key}
	 */
	public static <T> T get(Map<String, Object> inputResponses, String key, Class<T> type, McpJsonMapper jsonMapper) {
		if (inputResponses == null) {
			return null;
		}
		Object raw = inputResponses.get(key);
		return raw == null ? null : jsonMapper.convertValue(raw, type);
	}

}
