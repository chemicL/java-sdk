/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server;

/**
 * Seals and opens MRTR {@code requestState}. The spec treats inbound {@code requestState}
 * as attacker-controlled input: if it drives authorization or business logic, it MUST be
 * integrity-protected and rejected when verification fails. {@code McpServer} calls this
 * on every retry of {@code tools/call}, {@code resources/read} and {@code prompts/get} so
 * handlers only ever see verified, plaintext state.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface RequestStateCodec {

	/**
	 * Seal {@code state} for the request in {@code ctx} before it goes out in an
	 * {@code InputRequiredResult}.
	 */
	String seal(McpRequestContext ctx, String state);

	/**
	 * Open a previously sealed value, verifying it was produced for this same principal,
	 * method and primitive and has not expired.
	 * @throws io.modelcontextprotocol.modern.McpError ({@code -32602}) if verification
	 * fails
	 */
	String open(McpRequestContext ctx, String sealed);

}
