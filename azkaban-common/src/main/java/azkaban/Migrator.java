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
import static com.google.common.base.Preconditions.checkState;

import azkaban.project.JdbcProjectLoader;
import azkaban.project.JdbcProjectLoader.Result;
import azkaban.project.ProjectFileHandler;
import azkaban.spi.AzkabanException;
import azkaban.spi.Storage;
import azkaban.spi.StorageMetadata;
import azkaban.storage.DatabaseStorage;
import azkaban.storage.StorageImplementationType;
import azkaban.utils.Md5Hasher;
import azkaban.utils.Props;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.binary.Hex;


public class Migrator {

  final Props props;
  final JdbcProjectLoader jdbcProjectLoader;
  final DatabaseStorage databaseStorage;
  final Storage storage;

  final List<Result> failed = new ArrayList<>();

  @Inject
  public Migrator(Props props, JdbcProjectLoader jdbcProjectLoader,
      DatabaseStorage databaseStorage,
      Storage storage) {
    this.props = props;
    this.jdbcProjectLoader = jdbcProjectLoader;
    this.databaseStorage = databaseStorage;
    this.storage = storage;
  }

  private void migrate() {
    System.out.println("=====================================================");
    System.out.println(" Migration starting...");
    System.out.println("=====================================================");

    List<JdbcProjectLoader.Result> allActiveProjects = jdbcProjectLoader
        .fetchProjectsForMigration();
    System.out.println("fetched all migratable projects. #: " + allActiveProjects.size());
    for (int i = 0; i < allActiveProjects.size(); i++) {
      Result r = allActiveProjects.get(i);
      System.out.println("--------------------------------------------------------------------");
      System.out.printf("Migrating (%d of %d) :: %s%n", i + 1, allActiveProjects.size(), r);

      try {
        migrateProject(r);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Migration failed for r: " + r);
        failed.add(r);
      }
    }

    printFailed();
    System.out.println("=====================================================");
    System.out.println(" Migration complete.");
    System.out.println("=====================================================");
  }

  private void printFailed() {
    if (failed.size() > 0) {
      System.err.println("Migration Failed for ::");
      for (Result r : failed) {
        System.err.println(r);
      }
    }
  }

  private void migrateProject(Result r) throws IOException {
    final String key = String.format("%s/%s-%s.zip", r.id, r.id, string(r.md5));

    if (!storage.contains(key)) {
      long start = System.currentTimeMillis();
      final ProjectFileHandler pfh = jdbcProjectLoader.getUploadedFile(r.id, r.version);
      System.out.printf("Received file: %s [ %d KB] in %d sec%n",
          pfh.getLocalFile().getAbsolutePath(),
          pfh.getLocalFile().length() / 1024,
          (System.currentTimeMillis() - start) / 1000
      );

      StorageMetadata metadata = new StorageMetadata(r.id, r.version, "", r.md5);
      System.out.println("Metadata: " + metadata);

      start = System.currentTimeMillis();
      final String resourceId = storage.put(metadata, pfh.getLocalFile());
      System.out.printf("Stored file in %d sec%n",
          (System.currentTimeMillis() - start) / 1000
      );

      checkState(key.equals(resourceId));

      final InputStream inputStream = storage.get(key);
      final byte[] actual = Md5Hasher.md5Hash(inputStream);
      checkState(Arrays.equals(actual, r.md5), String
          .format("Hash Mismatch error for %s Found: %s, Expected: %s", r, string(actual),
              string(r.md5)));
    } else {
      System.out.println("File already present: " + r);
    }

    if (!key.equals(r.resourceId)) {
      System.out.printf("Updated ResourceId from: %s to %s%n", r.resourceId, key);
      jdbcProjectLoader.updateResourceId(r.id, r.version, key);
    }
    System.out.println("Migration complete for r: " + r);
  }

  private static String string(byte[] bytes) {
    return new String(Hex.encodeHex(bytes));
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: java -jar <jar> <migrate.properties>");
      System.exit(1);
    }

    final File propsFile = new File(args[0]);
    checkArgument(propsFile.exists());
    try {
      final Props props = new Props(null, propsFile);
      final StorageImplementationType type = StorageImplementationType
          .valueOf(props.getString("migration.type", "LOCAL"));
      System.out.println("**********************************************");
      System.out.println(" Migration to Storage type: " + type);
      System.out.println("**********************************************");

      final Injector injector = Guice.createInjector(new AzkabanCommonModule(props));
      new Migrator(
          props,
          injector.getInstance(JdbcProjectLoader.class),
          injector.getInstance(DatabaseStorage.class),
          injector.getInstance(type.getImplementationClass())
      ).migrate();
    } catch (IOException e) {
      e.printStackTrace();
      throw new AzkabanException(e);
    }
  }
}
