package azkaban.security;

import javax.annotation.Nonnull;
import java.security.KeyStore;

/**
 * Fetch and inject user secret key into custom credential object as well as use in-memory KeyStore.
 */
public interface CredentialProviderWithKeyStore extends CredentialProvider {
    /**
     * get KeyStore to be reused within an Azkaban Flow
     *
     * @return KeyStore
     */
    public KeyStore getKeyStore();

    /**
     * set KeyStore
     *
     * @param keyStore Object
     */
    public void setKeyStore(final @Nonnull KeyStore keyStore);
}
