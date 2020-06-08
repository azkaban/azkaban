/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban;

import static azkaban.Constants.ConfigurationKeys.HADOOP_CONF_DIR_PATH;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import azkaban.cachedhttpfilesystem.CachedHttpFileSystem;
import azkaban.spi.AzkabanException;
import azkaban.storage.HdfsAuth;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Place Hadoop dependencies in this module. Since Hadoop is not included in the Azkaban Runtime
 * dependency, we only install this module when Hadoop related injection (e.g., HDFS storage) is
 * needed.
 */
public class HadoopModule extends AbstractModule {
  private static final String CHTTP_SCHEME = "chttp";
  private static final String LOCAL_SCHEME = "file";
  private static final String HDFS_SCHEME = "hdfs";

  private static final String HDFS_FS_IMPL_CONFIG = "fs.hdfs.impl";
  private static final String ABSTRACT_FS_IMPL_CONFIG = "fs.AbstractFileSystem.hdfs.impl";

  // These classes will be used in case a value is not already provided the resource files
  private static final String FALLBACK_HDFS_IMPL_CLASSS =
      org.apache.hadoop.hdfs.DistributedFileSystem.class.getName();
  private static final String FALLBACK_ABSTRACT_FS_IMPL_CLASS =
      org.apache.hadoop.fs.Hdfs.class.getName();

  private static final Logger log = LoggerFactory.getLogger(HadoopModule.class);
  private final Props props;

  HadoopModule(final Props props) {
    this.props = props;
  }

  @Inject
  @Provides
  @Singleton
  @Named("hdfsConf")
  public Configuration createHDFSConfiguration() {
    final String hadoopConfDirPath = requireNonNull(this.props.get(HADOOP_CONF_DIR_PATH));

    final File hadoopConfDir = new File(requireNonNull(hadoopConfDirPath));
    checkArgument(hadoopConfDir.exists() && hadoopConfDir.isDirectory());

    final Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "core-site.xml"));
    conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "hdfs-site.xml"));
    if (conf.get(HDFS_FS_IMPL_CONFIG) == null) {
      conf.set(HDFS_FS_IMPL_CONFIG, FALLBACK_HDFS_IMPL_CLASSS);
    }
    if (conf.get(ABSTRACT_FS_IMPL_CONFIG) == null) {
      conf.set( ABSTRACT_FS_IMPL_CONFIG, FALLBACK_ABSTRACT_FS_IMPL_CLASS);
    }

    // These won't result in excessive logging as we are in a singleton context
    log.info("Implementation of {} is {}", HDFS_FS_IMPL_CONFIG, conf.get(HDFS_FS_IMPL_CONFIG));
    log.info("Implementation of {} is {}", ABSTRACT_FS_IMPL_CONFIG, conf.get(ABSTRACT_FS_IMPL_CONFIG));

    return conf;
  }

  @Inject
  @Provides
  @Singleton
  @Named("httpConf")
  public Configuration createHTTPConfiguration(final AzkabanCommonModuleConfig azConfig) {
    // NOTE (for the future): If we want to permanently remove the caching layer and simply pull dependencies
    // directly from the HTTP origin, swap out CachedHttpFileSystem for Hadoop's native HttpFileSystem
    // by editing this configuration here. In addition it will no longer be necessary to have two
    // separate createHDFSCachedHttpFileSystem and createLocalCachedHttpFileSystem methods but we can
    // just have one method createHttpFileSystem that creates one uncached HttpFileSystem. LocalHadoopStorage
    // and HdfsStorage constructors can be updated to inject the same HttpFileSystem instead of different versions
    // like we do right now: the locally cached version for LocalHadoopStorage and the hdfs cached version
    // for HdfsStorage.
    final Configuration conf = new Configuration(false);
    conf.set("fs.chttp.impl", azkaban.cachedhttpfilesystem.CachedHttpFileSystem.class.getName());
    boolean cachingEnabled = azConfig.getDependencyCachingEnabled();
    if (cachingEnabled) {
      // If caching is not disabled BUT the cache dependency root URI is not specified, return null
      // for this configuration (indicating this configuration cannot be generated - thin archives
      // should be disabled)
      if (azConfig.getCacheDependencyRootUri() == null) {
        return null;
      }
      // If caching is enabled, tell the CachedHttpFileSystem where to cache its files
      conf.set(CachedHttpFileSystem.CACHE_ROOT_URI, azConfig.getCacheDependencyRootUri().toString());
    } else {
      // If caching is disabled, tell the CachedHttpFileSystem to disable caching
      conf.set(CachedHttpFileSystem.CACHE_ENABLED_FLAG, "false");
    }
    return conf;
  }

  @Inject
  @Provides
  @Singleton
  @Named("localConf")
  public Configuration createLocalConfiguration() {
    final Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }

  @Inject
  @Provides
  @Singleton
  @Named("hdfsFileContext")
  public FileContext createHDFSFileContext(@Named("hdfsConf") final Configuration hdfsConf, final HdfsAuth auth) {
    try {
      auth.authorize();
      return FileContext.getFileContext(hdfsConf);
    } catch (final IOException e) {
      log.error("Unable to initialize HDFS FileContext.", e);
      throw new AzkabanException(e);
    }
  }

  @Inject
  @Provides
  @Singleton
  @Named("hdfs_cached_httpFS")
  public FileSystem createHDFSCachedHttpFileSystem(@Named("hdfsConf") final Configuration hdfsConf,
      @Named("httpConf") @Nullable final Configuration httpConf, final HdfsAuth auth, final AzkabanCommonModuleConfig azConfig) {
    if (httpConf == null) {
      return null;
    }

    // If caching is enabled, ensure the URI where cached files will be stored has the correct scheme
    // and has an authority.
    if (azConfig.getDependencyCachingEnabled()) {
      validateURI(azConfig.getCacheDependencyRootUri(), HDFS_SCHEME, true);
    }

    final Configuration finalConf = new Configuration(false);
    finalConf.addResource(hdfsConf);
    finalConf.addResource(httpConf);

    auth.authorize();
    return getCachedHttpFileSystem(finalConf, azConfig);
  }

  @Inject
  @Provides
  @Singleton
  @Named("local_cached_httpFS")
  public FileSystem createLocalCachedHttpFileSystem(@Named("localConf") final Configuration localConf,
      @Named("httpConf") @Nullable final Configuration httpConf, final AzkabanCommonModuleConfig azConfig) {
    if (httpConf == null) {
      return null;
    }

    // If caching is enabled, ensure the URI where cached files will be stored has the correct scheme.
    if (azConfig.getDependencyCachingEnabled()) {
      validateURI(azConfig.getCacheDependencyRootUri(), LOCAL_SCHEME, false);
    }

    final Configuration finalConf = new Configuration(false);
    finalConf.addResource(localConf);
    finalConf.addResource(httpConf);

    return getCachedHttpFileSystem(finalConf, azConfig);
  }

  private static FileSystem getCachedHttpFileSystem(final Configuration conf, final AzkabanCommonModuleConfig azConfig) {
    // Ensure the necessary props are not specified to enable CachedHttpFileSystem
    if (azConfig.getOriginDependencyRootUri() == null) {
      return null;
    }

    // Ensure the origin URI has the correct scheme and has an authority.
    validateURI(azConfig.getOriginDependencyRootUri(), CHTTP_SCHEME, true);

    try {
      return FileSystem.get(azConfig.getOriginDependencyRootUri(), conf);
    } catch (final IOException e) {
      log.error("Unable to initialize CachedHttpFileSystem.", e);
      throw new AzkabanException(e);
    }
  }

  /**
   * Ensure a URI is valid for a given scheme and contains an authority (if required).
   */
  private static void validateURI(final URI uri, final String scheme, final boolean mustHaveAuthority) {
    if (mustHaveAuthority) {
      requireNonNull(uri.getAuthority(), "URI must have host:port mentioned.");
    }
    checkArgument(scheme.equals(uri.getScheme()));
  }

  @Override
  protected void configure() {
  }
}
