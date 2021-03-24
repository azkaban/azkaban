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
import azkaban.storage.AbstractHdfsAuth;
import azkaban.utils.Utils;
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

  public static final String HADOOP_CONF = "hadoopConf";
  public static final String HADOOP_FS_AUTH = "hadoopFSAuth";
  public static final String HADOOP_FILE_CONTEXT = "hadoopFileContext";

  public static final String HTTP_CONF = "httpConf";
  public static final String LOCAL_CONF = "localConf";
  public static final String LOCAL_CACHED_HTTP_FS = "local_cached_httpFS";
  public static final String HDFS_CACHED_HTTP_FS = "hdfs_cached_httpFS";

  private static final String HADOOP_DEFAULT_FS_CONFIG_PROP = "fs.defaultFS";
  private static final String AZKABAN_HDFS_AUTH_IMPL_CLASS = "azkaban.hdfs.auth.impl.class";
  private static final String DEFAULT_AZKABAN_HDFS_AUTH_IMPL_CLASS =
      azkaban.storage.HdfsAuth.class.getName();

  private static final String CHTTP_SCHEME = "chttp";
  private static final String LOCAL_SCHEME = "file";
  private static final String HDFS_SCHEME = "hdfs";

  private static final Logger log = LoggerFactory.getLogger(HadoopModule.class);

  @Inject
  @Provides
  @Singleton
  @Named(HADOOP_CONF)
  public Configuration createHadoopConfiguration(final AzkabanCommonModuleConfig azConfig) {
    final String hadoopConfDirPath = requireNonNull(
        azConfig.getProps().getString(HADOOP_CONF_DIR_PATH));

    final File hadoopConfDir = new File(requireNonNull(hadoopConfDirPath));
    checkArgument(hadoopConfDir.exists() && hadoopConfDir.isDirectory());

    final Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "core-site.xml"));
    conf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "hdfs-site.xml"));

    log.info("Hadoop default file system is {}", conf.get(HADOOP_DEFAULT_FS_CONFIG_PROP));

    return conf;
  }

  /**
   * This method is used to instantiate the class that performs authentication and authorization
   * with the Hadoop file system. To provide a custom class the property {@link
   * #AZKABAN_HDFS_AUTH_IMPL_CLASS} should be set. If this property is not configured then {@link
   * azkaban.storage.HdfsAuth} will be used as the default.
   *
   * @param azConfig
   * @param conf
   * @return instance of {@link AbstractHdfsAuth}
   */
  @Inject
  @Provides
  @Singleton
  @Named(HADOOP_FS_AUTH)
  public AbstractHdfsAuth createHdfsAuth(final AzkabanCommonModuleConfig azConfig,
      @Named(HADOOP_CONF) final Configuration conf) {
    final String hdfsAuthImplClass = azConfig.getProps()
        .getString(AZKABAN_HDFS_AUTH_IMPL_CLASS, DEFAULT_AZKABAN_HDFS_AUTH_IMPL_CLASS);
    try {
      final Class<?> hdfsAuthClass =
          HadoopModule.class.getClassLoader().loadClass(hdfsAuthImplClass);
      log.info("Loading hdfs auth " + hdfsAuthClass.getName());
      return (AbstractHdfsAuth)
          Utils.callConstructor(hdfsAuthClass, azConfig.getProps(), conf);
    } catch (final ClassNotFoundException e) {
      log.error("Could not instantiate hdfsAuth: ", e);
      throw new AzkabanException("Failed to create object of hdfsAuth!"
          + e.getCause(), e);
    }
  }

  @Inject
  @Provides
  @Singleton
  @Named(HTTP_CONF)
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
    final boolean cachingEnabled = azConfig.getDependencyCachingEnabled();
    if (cachingEnabled) {
      // If caching is not disabled BUT the cache dependency root URI is not specified, return null
      // for this configuration (indicating this configuration cannot be generated - thin archives
      // should be disabled)
      if (azConfig.getCacheDependencyRootUri() == null) {
        return null;
      }
      // If caching is enabled, tell the CachedHttpFileSystem where to cache its files
      conf.set(CachedHttpFileSystem.CACHE_ROOT_URI,
          azConfig.getCacheDependencyRootUri().toString());
    } else {
      // If caching is disabled, tell the CachedHttpFileSystem to disable caching
      conf.set(CachedHttpFileSystem.CACHE_ENABLED_FLAG, "false");
    }
    return conf;
  }

  @Inject
  @Provides
  @Singleton
  @Named(LOCAL_CONF)
  public Configuration createLocalConfiguration() {
    final Configuration conf = new Configuration(false);
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }

  @Inject
  @Provides
  @Singleton
  @Named(HADOOP_FILE_CONTEXT)
  public FileContext createHadoopFileContext(
      @Named(HADOOP_CONF) final Configuration conf,
      @Named(HADOOP_FS_AUTH) final AbstractHdfsAuth auth) {
    try {
      auth.authorize();
      return FileContext.getFileContext(conf);
    } catch (final IOException e) {
      log.error("Unable to initialize Hadoop FileContext.", e);
      throw new AzkabanException(e);
    }
  }

  @Inject
  @Provides
  @Singleton
  @Named(HDFS_CACHED_HTTP_FS)
  public FileSystem createHDFSCachedHttpFileSystem(
      @Named(HADOOP_CONF) final Configuration conf,
      @Named(HTTP_CONF) @Nullable final Configuration httpConf,
      @Named(HADOOP_FS_AUTH) final AbstractHdfsAuth auth,
      final AzkabanCommonModuleConfig azConfig) {
    if (httpConf == null) {
      return null;
    }

    // If caching is enabled, ensure the URI where cached files will be stored has the correct scheme
    // and has an authority.
    if (azConfig.getDependencyCachingEnabled()) {
      validateURI(azConfig.getCacheDependencyRootUri(), HDFS_SCHEME, true);
    }

    final Configuration finalConf = new Configuration(false);
    finalConf.addResource(conf);
    finalConf.addResource(httpConf);

    auth.authorize();
    return getCachedHttpFileSystem(finalConf, azConfig);
  }

  @Inject
  @Provides
  @Singleton
  @Named(LOCAL_CACHED_HTTP_FS)
  public FileSystem createLocalCachedHttpFileSystem(
      @Named(LOCAL_CONF) final Configuration localConf,
      @Named(HTTP_CONF) @Nullable final Configuration httpConf,
      final AzkabanCommonModuleConfig azConfig) {
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

  private static FileSystem getCachedHttpFileSystem(final Configuration conf,
      final AzkabanCommonModuleConfig azConfig) {
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
  private static void validateURI(final URI uri, final String scheme,
      final boolean mustHaveAuthority) {
    if (mustHaveAuthority) {
      requireNonNull(uri.getAuthority(), "URI must have host:port mentioned.");
    }
    checkArgument(scheme.equals(uri.getScheme()));
  }

  @Override
  protected void configure() {
  }
}
