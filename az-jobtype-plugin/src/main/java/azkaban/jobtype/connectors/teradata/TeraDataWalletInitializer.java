/*
 * Copyright (C) 2015-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.jobtype.connectors.teradata;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.jobtype.javautils.FileUtils;

/**
 * Consists of static methods mainly because all methods should behave as singleton.
 */
public class TeraDataWalletInitializer {
  private static Logger logger = Logger.getLogger(TeraDataWalletInitializer.class);
  private static String UNJAR_DIR_NAME = "unjar_tdch";
  private static File tdchJarExtractedDir;

  /**
   * As of TDCH 1.4.1, integration with Teradata wallet only works with hadoop jar command line command.
   * This is mainly because TDCH depends on the behavior of hadoop jar command line which extracts jar file
   * into hadoop tmp folder.
   *
   * This method will extract tdchJarfile and place it into temporary folder, and also add jvm shutdown hook
   * to delete the directory when JVM shuts down.
   * @param tdchJarFile TDCH jar file.
   */
  public static void initialize(File tmpDir, File tdchJarFile) {
    synchronized (TeraDataWalletInitializer.class) {
      if (tdchJarExtractedDir != null) {
        return;
      }

      if (tdchJarFile == null) {
        throw new IllegalArgumentException("TDCH jar file cannot be null.");
      }
      if (!tdchJarFile.exists()) {
        throw new IllegalArgumentException("TDCH jar file does not exist. " + tdchJarFile.getAbsolutePath());
      }
      try {
        //Extract TDCH jar.
          File unJarDir = createUnjarDir(new File(tmpDir.getAbsolutePath() + File.separator + UNJAR_DIR_NAME));
          JarFile jar = new JarFile(tdchJarFile);
          Enumeration<JarEntry> enumEntries = jar.entries();

          while (enumEntries.hasMoreElements()) {
            JarEntry srcFile = enumEntries.nextElement();
            File destFile = new File(unJarDir + File.separator + srcFile.getName());
            if (srcFile.isDirectory()) { // if its a directory, create it
              destFile.mkdir();
              continue;
            }

            InputStream is = jar.getInputStream(srcFile);
            BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(destFile));
            IOUtils.copy(is, os);
            close(os);
            close(is);
          }
          jar.close();
          tdchJarExtractedDir = unJarDir;
      } catch (IOException e) {
        throw new RuntimeException("Failed while extracting TDCH jar file.", e);
      }
    }
    logger.info("TDCH jar has been extracted into directory: " + tdchJarExtractedDir.getAbsolutePath());
  }

  private static File createUnjarDir(final File unjarDir) throws IOException {
    logger.info("trying to create tdch unjar directory. " + unjarDir.getAbsolutePath());
    if(unjarDir.exists()) {
      logger.info("Delete existing tdch unjar directory. " + unjarDir.getAbsolutePath());
      FileUtils.deleteFileOrDirectory(unjarDir);
    }
    unjarDir.mkdir();
    Runtime.getRuntime().addShutdownHook( new Thread() {
      @Override
      public void run() {
        if(unjarDir.exists()) {
          logger.info("JVM is shutting down. Deleting a folder where TDCH jar is extracted. " + unjarDir.getAbsolutePath());
          FileUtils.tryDeleteFileOrDirectory(unjarDir);
        }
      }
    });
    logger.info("Created tdch unjar directory. " + unjarDir.getAbsolutePath());
    return unjarDir;
  }

  private static void close(Closeable closeable) throws IOException {
    if(closeable != null) {
      closeable.close();
    }
  }

  public static String getTdchUnjarFolder() {
    if(tdchJarExtractedDir == null) {
      throw new IllegalStateException("Not initialized yet.");
    }
    return tdchJarExtractedDir.getAbsolutePath();
  }
}
