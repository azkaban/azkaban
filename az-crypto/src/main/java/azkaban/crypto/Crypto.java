/*
 * Copyright (C) 2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


/**
 * Crypto class that actually delegates to version specific implementation of ICrypto interface. In
 * other words, it's a factory method class that implements ICrypto interface
 */
public class Crypto implements ICrypto {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final Map<Version, ICrypto> cryptos;

  public Crypto() {
    this.cryptos = ImmutableMap.<Version, ICrypto>builder()
        .put(Version.V1_0, new CryptoV1())
        .put(Version.V1_1, new CryptoV1_1())
        .build();
  }

  public static String encode(final String s) {
    return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
  }

  public static String decode(final String s) {
    return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
  }

  @Override
  public String encrypt(final String plaintext, final String passphrase,
      final Version cryptoVersion) {
    Preconditions.checkNotNull(cryptoVersion, "Crypto version is required.");
    Preconditions.checkArgument(!StringUtils.isEmpty(plaintext), "plaintext should not be empty");
    Preconditions.checkArgument(!StringUtils.isEmpty(passphrase), "passphrase should not be empty");

    final ICrypto crypto = this.cryptos.get(cryptoVersion);
    Preconditions.checkNotNull(crypto, cryptoVersion + " is not supported.");
    return crypto.encrypt(plaintext, passphrase, cryptoVersion);
  }

  @Override
  public String decrypt(final String cipheredText, final String passphrase) {
    Preconditions
        .checkArgument(!StringUtils.isEmpty(cipheredText), "cipheredText should not be empty");
    Preconditions.checkArgument(!StringUtils.isEmpty(passphrase), "passphrase should not be empty");

    try {
      final String jsonStr = decode(cipheredText);
      final JsonNode json = MAPPER.readTree(jsonStr);
      final String ver = json.get(VERSION_IDENTIFIER).asText();

      final ICrypto crypto = this.cryptos.get(Version.fromVerString(ver));
      Preconditions.checkNotNull(crypto);
      return crypto.decrypt(cipheredText, passphrase);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}