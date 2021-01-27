/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.imagemgmt.version;

import azkaban.imagemgmt.models.ImageVersion.State;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class VersionSetBuilderTest {

  @Test
  public void testVersionSet() throws IOException {
    final String EXPECTED_JSON = "{\"key1\":{\"version\":\"value1\",\"path\":\"path1\","
        + "\"state\":\"ACTIVE\"},\"key2\":{\"version\":\"value2\",\"path\":\"path2\","
        + "\"state\":\"ACTIVE\"},\"key3\":{\"version\":\"value3\",\"path\":\"path3\","
        + "\"state\":\"ACTIVE\"}}";
    final String EXPECTED_MD5 = "43966138aebfdc4438520cc5cd2aefa8";

    // Test if the elements are ordered by keys, not by order of addition
    VersionSetLoader versionSetLoader = Mockito.mock(VersionSetLoader.class);
    Mockito.when(versionSetLoader.getVersionSet(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(java.util.Optional.of(new VersionSet(EXPECTED_JSON, EXPECTED_MD5, 1)));
    VersionSet versionSet = new VersionSetBuilder(versionSetLoader)
        .addElement("key1", new VersionInfo("value1", "path1", State.ACTIVE))
        .addElement("key3", new VersionInfo("value3", "path3", State.ACTIVE))
        .addElement("key2", new VersionInfo("value2", "path2", State.ACTIVE))
        .build();
    Assert.assertEquals(EXPECTED_JSON, versionSet.getVersionSetJsonString());
    Assert.assertEquals("43966138aebfdc4438520cc5cd2aefa8", versionSet.getVersionSetMd5Hex());
    Assert.assertEquals(1, versionSet.getVersionSetId());
    Assert.assertEquals("value3", versionSet.getVersion("key3").get().getVersion());
    Assert.assertEquals("value1", versionSet.getVersion("key1").get().getVersion());
    Assert.assertEquals("value2", versionSet.getVersion("key2").get().getVersion());
    Assert.assertEquals(false, versionSet.getVersion("key4").isPresent());
  }

  @Test
  public void testCaseInsensitiveVersionSet() throws IOException {
    final String EXPECTED_JSON = "{\"key1\":{\"version\":\"value1\",\"path\":\"path1\","
        + "\"state\":\"ACTIVE\"},\"key2\":{\"version\":\"value2\",\"path\":\"path2\","
        + "\"state\":\"ACTIVE\"},\"key3\":{\"version\":\"value3\",\"path\":\"path3\","
        + "\"state\":\"ACTIVE\"},\"Key4\":{\"version\":\"value4\",\"path\":\"path4\","
        + "\"state\":\"ACTIVE\"},\"KEY5\":{\"version\":\"VALUE5\",\"path\":\"path5\","
        + "\"state\":\"ACTIVE\"},\"KEY6\":{\"version\":\"VALUE6\",\"path\":\"path6\","
        + "\"state\":\"ACTIVE\"}}";
    final String EXPECTED_MD5 = "43966138aebfdc4438520cc5cd2aefa8";

    // Test if the elements are ordered by keys, not by order of addition
    VersionSetLoader versionSetLoader = Mockito.mock(VersionSetLoader.class);
    Mockito.when(versionSetLoader.getVersionSet(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(java.util.Optional.of(new VersionSet(EXPECTED_JSON, EXPECTED_MD5, 1)));
    VersionSet versionSet = new VersionSetBuilder(versionSetLoader)
        .addElement("key1", new VersionInfo("value1", "path1", State.ACTIVE))
        .addElement("key3", new VersionInfo("value3", "path3", State.ACTIVE))
        .addElement("key2", new VersionInfo("value2", "path2", State.ACTIVE))
        .addElement("Key1", new VersionInfo("value1", "path1", State.ACTIVE))
        .addElement("KEY2", new VersionInfo("VALUE2", "path2", State.ACTIVE))
        .addElement("Key4", new VersionInfo("value4", "path4", State.ACTIVE))
        .addElement("KEY5", new VersionInfo("VALUE5", "path5", State.ACTIVE))
        .addElement("KEY6", new VersionInfo("VALUE6", "path6", State.ACTIVE))
        .build();
    Assert.assertEquals(EXPECTED_JSON, versionSet.getVersionSetJsonString());
    Assert.assertEquals("value2", versionSet.getVersion("KEY2").get().getVersion());
    Assert.assertEquals("VALUE5", versionSet.getVersion("key5").get().getVersion());
    Assert.assertEquals("VALUE6", versionSet.getVersion("KEY6").get().getVersion());
    Assert.assertEquals("43966138aebfdc4438520cc5cd2aefa8", versionSet.getVersionSetMd5Hex());
    Assert.assertEquals(1, versionSet.getVersionSetId());
    Assert.assertEquals("value3", versionSet.getVersion("key3").get().getVersion());
    Assert.assertEquals("value1", versionSet.getVersion("key1").get().getVersion());
    Assert.assertEquals("value2", versionSet.getVersion("key2").get().getVersion());
    Assert.assertEquals(false, versionSet.getVersion("key7").isPresent());
  }
}
