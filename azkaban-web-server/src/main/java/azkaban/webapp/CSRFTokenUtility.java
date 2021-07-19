/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.webapp;

import azkaban.server.session.Session;
import azkaban.utils.StringUtils;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Singleton Class for generating CSRF token from session-id. This class uses "HmacSHA256" algorithm
 * and a secret-key to get the hashed value of session-id, which will be used as the CSRF token.
 */
public class CSRFTokenUtility {

  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);
  private static final String ALGORITHM = "HmacSHA256";
  private Mac hashFunction;
  private static CSRFTokenUtility instance;

  private CSRFTokenUtility() {
    // To be instantiated only by this class
  }

  /**
   * @return A singleton instance of CSRFTokenUtility
   */
  public static synchronized CSRFTokenUtility getCSRFTokenUtility() {
    if (null == instance) {
      CSRFTokenUtility csrfTokenUtility = new CSRFTokenUtility();
      try {
        csrfTokenUtility.initHashFunction();
      } catch (NoSuchAlgorithmException | InvalidKeyException e) {
        logger.info("Unable to initialize the hash function for creating CSRF Token", e);
        return null;
      }
      instance = csrfTokenUtility;
    }
    return instance;
  }

  /**
   * @param session containing session-id
   * @return CSRF token derived from session-id
   */
  public String getCSRFTokenFromSession(Session session) {
    if (null == session || StringUtils.isEmpty(session.getSessionId())) {
      return null;
    }
    String sessionId = session.getSessionId();
    return getCSRFTokenFromSessionId(sessionId);
  }

  /**
   * @param sessionId
   * @return CSRF token derived from session-id
   */
  private String getCSRFTokenFromSessionId(String sessionId) {
    byte[] bytes = sessionId.getBytes(UTF_8);
    byte[] hashedSessionId = this.hashFunction.doFinal(bytes);
    return Base64.getEncoder().encodeToString(hashedSessionId);
  }

  /**
   * @return Eight bytes SecretKey to be used for initializing HmacSHA256
   */
  private static byte[] getSecretKey() {
    SecureRandom randomGen = new SecureRandom();
    byte[] secretKey = new byte[8];
    randomGen.nextBytes(secretKey);
    return secretKey;
  }

  private void initHashFunction() throws NoSuchAlgorithmException, InvalidKeyException {
    byte[] secretKey = getSecretKey();
    initHashFunction(secretKey);
  }

  private void initHashFunction(byte[] secretKey)
      throws NoSuchAlgorithmException, InvalidKeyException {
    Mac mac = Mac.getInstance(ALGORITHM);
    SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey, ALGORITHM);
    mac.init(secretKeySpec);
    this.hashFunction = mac;
  }

  /**
   * @param sessionId
   * @param csrfTokenFromRequest
   * @return True if the csrfTokenFromRequest matches with the CSRFToken generated from session-id,
   * otherwise false
   */
  public boolean validateCSRFToken(String sessionId, String csrfTokenFromRequest) {
    String csrfTokenFromSessionId = getCSRFTokenFromSessionId(sessionId);
    if (StringUtils.isEmpty(csrfTokenFromSessionId)) {
      return false;
    }
    return MessageDigest.isEqual(
        csrfTokenFromRequest.getBytes(UTF_8),
        csrfTokenFromSessionId.getBytes(UTF_8));
  }
}
