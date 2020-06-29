/*
 * Copyright 2014 LinkedIn Corp.
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
 */

package azkaban.project;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.flow.Flow;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DirectoryFlowLoaderTest {

  private Project project;

  private static File decompressTarBZ2(InputStream is) throws IOException {
    File outputDir = Files.createTempDir();

    try (TarArchiveInputStream tais = new TarArchiveInputStream(
        new BZip2CompressorInputStream(is))) {
      TarArchiveEntry entry;
      while ((entry = tais.getNextTarEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }

        File outputFile = new File(outputDir, entry.getName());
        File parent = outputFile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }

        try (FileOutputStream os = new FileOutputStream(outputFile)) {
          IOUtils.copy(tais, os);
        }
      }

      return outputDir;
    }
  }

  @Before
  public void setUp() {
    this.project = new Project(11, "myTestProject");
  }

  @Test
  public void testDirectoryLoad() {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("exectest1"));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(5, loader.getFlowMap().size());
    Assert.assertEquals(2, loader.getPropsList().size());
    Assert.assertEquals(14, loader.getJobPropsMap().size());
  }

  @Test
  public void testLoadEmbeddedFlow() {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(2, loader.getFlowMap().size());
    Assert.assertEquals(0, loader.getPropsList().size());
    Assert.assertEquals(9, loader.getJobPropsMap().size());
  }

  @Test
  public void testRecursiveLoadEmbeddedFlow() {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded_bad"));
    for (final String error : loader.getErrors()) {
      System.out.println(error);
    }

    // Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
    Assert.assertEquals(3, loader.getErrors().size());
  }

  @Test
  public void testMassiveFlow() throws Exception {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    File projectDir = null;
    try (InputStream is = new FileInputStream(
        ExecutionsTestUtil.getDataRootDir() + "/massive-flows/massive-flows.tar.bz2")) {
      projectDir = decompressTarBZ2(is);

      loader.loadProjectFlow(this.project, projectDir);
      Assert.assertEquals(0, loader.getErrors().size());
      Assert.assertEquals(185, loader.getFlowMap().size());
      Assert.assertEquals(0, loader.getPropsList().size());
      Assert.assertEquals(7121, loader.getJobPropsMap().size());
    } finally {
      if (projectDir != null) {
        MoreFiles.deleteRecursively(projectDir.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
      }
    }
  }
}
