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

import static com.google.common.base.Preconditions.checkArgument;

import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectFileHandler;
import azkaban.spi.AzkabanException;
import azkaban.spi.StorageMetadata;
import azkaban.storage.DatabaseStorage;
import azkaban.storage.LocalStorage;
import azkaban.utils.Props;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.codec.binary.Hex;


public class MigrateSqlToLocal {

  final Props props;
  final JdbcProjectLoader jdbcProjectLoader;
  final DatabaseStorage databaseStorage;
  final LocalStorage localStorage;

  @Inject
  public MigrateSqlToLocal(Props props, JdbcProjectLoader jdbcProjectLoader,
      DatabaseStorage databaseStorage,
      LocalStorage localStorage) {
    this.props = props;
    this.jdbcProjectLoader = jdbcProjectLoader;
    this.databaseStorage = databaseStorage;
    this.localStorage = localStorage;
  }

  private void migrate() {
    List<JdbcProjectLoader.Result> allActiveProjects = jdbcProjectLoader
        .fetchProjectsForMigration();
    System.out.println("fetched all migratable projects. #: " + allActiveProjects.size());
    for (JdbcProjectLoader.Result r : allActiveProjects) {
      final String key = String
          .format("%s/%s-%s.zip", r.id, r.id, new String(Hex.encodeHex(r.md5)));
      System.out.println("--------------------------------------------------------------------");
      System.out.println("Adding storage for " + r);
      if (!localStorage.contains(key)) {
        final ProjectFileHandler pfh = jdbcProjectLoader.getUploadedFile(r.id, r.version);
        System.out.println("Received file: " + pfh.getLocalFile().getAbsolutePath());

        StorageMetadata metadata = new StorageMetadata(r.id, r.version, "", r.md5);
        System.out.println("Metadata: " + metadata);
        final String resourceId = localStorage.put(metadata, pfh.getLocalFile());
        System.out.printf("ResourceId: %s\n", resourceId);
        jdbcProjectLoader.updateResourceId(r.id, r.version, resourceId);
        System.out.println("Migration complete for r: " + r);
      } else {
        System.out.println("Already present: " + r);
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: java -jar <jar> <migrate.properties>");
      System.exit(1);
    }

    final File propsFile = new File(args[0]);
    checkArgument(propsFile.exists());
    try {
      final Injector injector = Guice
          .createInjector(new AzkabanCommonModule(new Props(null, propsFile)));
      injector.getInstance(MigrateSqlToLocal.class).migrate();
    } catch (IOException e) {
      e.printStackTrace();
      throw new AzkabanException(e);
    }
  }
}
