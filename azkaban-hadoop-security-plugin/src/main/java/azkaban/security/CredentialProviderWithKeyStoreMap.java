package azkaban.security;

import java.security.KeyStore;
import java.util.Map;
import javax.annotation.Nonnull;


public interface CredentialProviderWithKeyStoreMap extends CredentialProvider {
  /**
   * get KeyStoreMap to be reused within an Azkaban Flow
   *
   * @return KeyStore
   */
  public Map<String, KeyStore> getKeyStoreMap();

  /**
   * Set KeyStoreMap to be reused within an Azkaban Flow
   *
   * @return KeyStore
   */
  public void setKeyStoreMap(final @Nonnull Map<String, KeyStore> keyStoreMap);

}