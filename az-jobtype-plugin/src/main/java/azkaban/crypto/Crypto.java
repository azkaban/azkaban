/*
 * Copyright (C) 2016 LinkedIn Corp. All rights reserved.
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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Crypto class that actually delegates to version specific implementation of ICrypto interface. In other words,
 * it's a factory method class that implements ICrypto interface
 */
public class Crypto implements ICrypto {
  private static final Logger logger = Logger.getLogger(Crypto.class);
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final Map<Version, ICrypto> cryptos;

  public Crypto() {
    this.cryptos = ImmutableMap.<Version, ICrypto>builder()
                                  .put(Version.V1_0, new CryptoV1())
                                  .put(Version.V1_1, new CryptoV1_1())
                                  .build();
  }

  @Override
  public String encrypt(String plaintext, String passphrase, Version cryptoVersion) {
    Preconditions.checkNotNull(cryptoVersion, "Crypto version is required.");
    Preconditions.checkArgument(!StringUtils.isEmpty(plaintext), "plaintext should not be empty");
    Preconditions.checkArgument(!StringUtils.isEmpty(passphrase), "passphrase should not be empty");

    ICrypto crypto = cryptos.get(cryptoVersion);
    Preconditions.checkNotNull(crypto, cryptoVersion + " is not supported.");
    return crypto.encrypt(plaintext, passphrase, cryptoVersion);
  }

  @Override
  public String decrypt(String cipheredText, String passphrase) {
    Preconditions.checkArgument(!StringUtils.isEmpty(cipheredText), "cipheredText should not be empty");
    Preconditions.checkArgument(!StringUtils.isEmpty(passphrase), "passphrase should not be empty");

    try {
      String jsonStr = decode(cipheredText);
      JsonNode json = MAPPER.readTree(jsonStr);
      String ver = json.get(ICrypto.VERSION_IDENTIFIER).asText();

      ICrypto crypto = cryptos.get(Version.fromVerString(ver));
      Preconditions.checkNotNull(crypto);
      return crypto.decrypt(cipheredText, passphrase);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String encode(String s) {
    return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
  }

  public static String decode(String s) {
    return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
  }
}
