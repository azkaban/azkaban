/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.reportal.util.tableau;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class URLResponseTest {


  @Test
  public void testURLResponse() throws Exception {
    final URLResponse urlResponse = mock(URLResponse.class);
    doNothing().when(urlResponse).refreshContents();
    when(urlResponse.indicatesSuccess()).thenCallRealMethod();
    when(urlResponse.indicatesError()).thenCallRealMethod();
    doCallRealMethod().when(urlResponse).setURLContents(anyString());

    urlResponse.setURLContents("Success");
    assertThat(urlResponse.indicatesSuccess()).isTrue();
    assertThat(urlResponse.indicatesError()).isFalse();

    urlResponse.setURLContents("Error");
    assertThat(urlResponse.indicatesError()).isTrue();
    assertThat(urlResponse.indicatesSuccess()).isFalse();
  }

}
