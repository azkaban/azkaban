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

/**
 * Encrypts plain text and decrypts ciphered text.
 */
public interface ICrypto {

  static final String VERSION_IDENTIFIER = "ver";

  /**
   * Encrypts plain text using pass phrase and crypto version.
   *
   * @param plaintext The plain text (secret) need to be encrypted.
   * @param passphrase Passphrase that will be used as a key to encrypt
   * @param cryptoVersion Version of this encryption.
   * @return A ciphered text, Base64 encoded.
   */
  public String encrypt(String plaintext, String passphrase, Version cryptoVersion);

  /**
   * Decrypts ciphered text.
   *
   * @param cipheredText Base64 encoded ciphered text
   * @param passphrase Passphrase that was used as a key to encrypt the ciphered text
   * @return plain text String
   */
  public String decrypt(String cipheredText, String passphrase);
}