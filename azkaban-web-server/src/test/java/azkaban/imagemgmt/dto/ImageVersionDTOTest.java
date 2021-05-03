/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.imagemgmt.dto;

import azkaban.imagemgmt.utils.ValidatorUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests image version validation logic.
 */
public class ImageVersionDTOTest {

  /**
   * Common code for valid versions
   * @param version the image version
   */
  public void testValidVersionInternal(final String version) {
    final ImageVersionDTO dto = new ImageVersionDTO();
    dto.setName("testImageType");
    List<String> validationErrors = new ArrayList<>();
    dto.setVersion(version);
    boolean result = ValidatorUtils.validateObject(dto, validationErrors, BaseDTO.ValidationOnCreate.class);
    Assert.assertTrue(result);
    Assert.assertTrue(validationErrors.isEmpty());
  }

  /**
   * Common code for invalid image versions
   * @param version the image version
   */
  public void testInvalidVersionInternal(final String version) {
    final ImageVersionDTO dto = new ImageVersionDTO();
    dto.setName("testImageType");
    List<String> validationErrors = new ArrayList<>();
    dto.setVersion(version);
    boolean result = ValidatorUtils.validateObject(dto, validationErrors, BaseDTO.ValidationOnCreate.class);
    Assert.assertFalse(result);
    Assert.assertFalse(validationErrors.isEmpty());
    Assert.assertEquals(1, validationErrors.size());
    final String err = "ImageVersion must be in major.minor.patch.hotfix format (ex. 0.1, 1.2, 1.2.5, 1.2.3.4 etc.).";
    Assert.assertEquals(err, validationErrors.get(0));
  }

  @Test
  public void testValidMajorVersion() {
    testValidVersionInternal("1");
  }

  @Test
  public void testValidMinorVersion() {
    testValidVersionInternal("1.2");
  }

  @Test
  public void testValidPatchVersion() {
    testValidVersionInternal("1.2.3");
  }

  @Test
  public void testValidHotfixVersion() {
    testValidVersionInternal("1.2.3.4");
  }

  @Test
  public void testInvalidVersions() {
    // Valid looking version but have more than 4 values. eg, 1.2.3.4.5
    testInvalidVersionInternal("1.2.3.4.5");
    // Invalid format of version
    testInvalidVersionInternal("1.2.");
  }
}
