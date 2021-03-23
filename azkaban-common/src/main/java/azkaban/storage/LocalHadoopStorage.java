package azkaban.storage;

import static azkaban.HadoopModule.LOCAL_CACHED_HTTP_FS;
import static azkaban.utils.StorageUtils.getTargetDependencyPath;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Dependency;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * LocalHadoopStorage is an extension of LocalStorage that adds support for dependency fetching.
 * LocalHadoopStorage depends on hadoop-common and can only be injected when hadoop-common is on the
 * classpath.
 */
public class LocalHadoopStorage extends LocalStorage {

  private static final Logger log = LoggerFactory.getLogger(LocalHadoopStorage.class);

  final FileSystem http;
  final URI dependencyRootUri;

  @Inject
  public LocalHadoopStorage(final AzkabanCommonModuleConfig config,
      @Named(LOCAL_CACHED_HTTP_FS) @Nullable final FileSystem http) {
    super(config);

    this.http = http; // May be null if thin archives is not enabled
    this.dependencyRootUri = config.getOriginDependencyRootUri();
  }

  @Override
  public InputStream getDependency(final Dependency dep) throws IOException {
    if (!dependencyFetchingEnabled()) {
      throw new UnsupportedOperationException("Dependency fetching is not enabled.");
    }

    return this.http.open(resolveAbsoluteDependencyURI(dep));
  }

  @Override
  public boolean dependencyFetchingEnabled() {
    return this.http != null;
  }

  @Override
  public String getDependencyRootPath() {
    return dependencyFetchingEnabled() ? this.dependencyRootUri.toString() : null;
  }

  private Path resolveAbsoluteDependencyURI(final Dependency dep) {
    return new Path(this.dependencyRootUri.toString(), getTargetDependencyPath(dep));
  }
}
