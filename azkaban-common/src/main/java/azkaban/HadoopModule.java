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

import azkaban.spi.AzkabanException;
import azkaban.storage.HdfsAuth;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import java.io.File;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Place Hadoop dependencies in this module. Since Hadoop is not included in the Azkaban Runtime
 * dependency, we only install this module when Hadoop related injection (e.g., HDFS storage) is
 * needed.
 */
public class HadoopModule extends AbstractModule{

  private static final Logger LOG = LoggerFactory.getLogger(HadoopModule.class);
  private final Props props;

  HadoopModule(final Props props) {
    this.props = props;
  }

  @Inject
  @Provides
  @Singleton
  public Configuration createHadoopConfiguration() {
    final String hadoopConfDirPath = requireNonNull(this.props.get(HADOOP_CONF_DIR_PATH));

    final File hadoopConfDir = new File(requireNonNull(hadoopConfDirPath));
    checkArgument(hadoopConfDir.exists() && hadoopConfDir.isDirectory());

    final Configuration hadoopConf = new Configuration(false);
    hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "core-site.xml"));
    hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "hdfs-site.xml"));
    hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    return hadoopConf;
  }

  @Inject
  @Provides
  @Singleton
  public FileSystem createHadoopFileSystem(final Configuration hadoopConf, final HdfsAuth auth) {
    try {
      auth.authorize();
      return FileSystem.get(hadoopConf);
    } catch (final IOException e) {
      LOG.error("Unable to initialize HDFS", e);
      throw new AzkabanException(e);
    }
  }

  @Override
  protected void configure() {
  }
}
