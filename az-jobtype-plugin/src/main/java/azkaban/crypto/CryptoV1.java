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

import org.apache.log4j.Logger;
import org.jasypt.encryption.pbe.PBEStringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

/**
 * Encrypts and decrypts using DES algorithm.
 * DES is not recommended by NIST, but this is particularly useful for the JRE environment
 * that haven't installed (or cannot install) JCE unlimited strength.
 */
public class CryptoV1 implements ICrypto {
  private static final Logger logger = Logger.getLogger(CryptoV1.class);

  private static final String CIPHERED_TEXT_KEY = "val";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String encrypt(String plaintext, String passphrase, Version cryptoVersion) {
    Preconditions.checkArgument(Version.V1_0.equals(cryptoVersion));

    String cipheredText = newEncryptor(passphrase).encrypt(plaintext);
    ObjectNode node = MAPPER.createObjectNode();
    node.put(CIPHERED_TEXT_KEY, cipheredText);
    node.put(ICrypto.VERSION_IDENTIFIER, Version.V1_0.versionStr());

    return Crypto.encode(node.toString());
  }

  @Override
  public String decrypt(String cipheredText, String passphrase) {
    try {
      JsonNode json = MAPPER.readTree(Crypto.decode(cipheredText));
      return newEncryptor(passphrase).decrypt(json.get(CIPHERED_TEXT_KEY).asText());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * DES algorithm
   * @param passphrase
   * @return
   */
  private PBEStringEncryptor newEncryptor(String passphrase) {
    StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
    encryptor.setPassword(passphrase);
    return encryptor;
  }
}
