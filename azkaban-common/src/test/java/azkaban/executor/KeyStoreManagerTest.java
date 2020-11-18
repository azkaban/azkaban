package azkaban.executor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.KeyStore;

public class KeyStoreManagerTest {

  private KeyStoreManager keyStoreManager;
  private KeyStore keyStore;

  @Before
  public void setup() {
    this.keyStore = null;
    keyStoreManager = KeyStoreManager.getInstance();
  }

  /**
   * Since the KeyStore is not set, it must be null
   */
  @Test
  public void getKeyStoreNull() {
    final KeyStore keyStore = keyStoreManager.getKeyStore();
    Assert.assertNull(keyStore);
  }

  /**
   * KeyStore is set, it must not be null, and must be same as input when retrieved.
   */
  @Test
  public void getKeyStoreNotNull() {
    this.keyStore = Mockito.mock(KeyStore.class);
    keyStoreManager.setKeyStore(this.keyStore);
    final KeyStore keyStore1 = keyStoreManager.getKeyStore();
    Assert.assertNotNull(keyStore1);
    Assert.assertEquals(this.keyStore, keyStore1);
  }
}
