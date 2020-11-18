package azkaban.executor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.security.KeyStore;

/**
 * KeyStoreManger is responsible to manage the KeyStore used during containerized execution of a
 * flow. The HadoopSecurityManager must provide the KeyStore to CredentialProvider
 */
public class KeyStoreManager {
  private static volatile KeyStoreManager ksmInstance = null;
  private final static Logger logger = LoggerFactory.getLogger(KeyStoreManager.class);

  private KeyStore keyStore = null;

  public static KeyStoreManager getInstance() {
    if (ksmInstance == null) {
      synchronized (KeyStoreManager.class) {
        if (ksmInstance == null) {
          logger.info("Getting a new instance of KeyStoreManager");
          ksmInstance = new KeyStoreManager();
        }
      }
    }
    logger.info("Getting existing instance of KeyStoreManager");
    return ksmInstance;
  }

  public KeyStore getKeyStore() {
    return this.keyStore;
  }

  public void setKeyStore(final @Nonnull KeyStore keyStore) {
    this.keyStore = keyStore;
  }
}
