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
 */
package azkaban.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test class for azkaban.utils.Props
 */
public class PropsTest {
  Props p = new Props();
  private static final String EXTRA_HCAT_CLUSTERS = "other_hcat_clusters";

  /* Test for getStringListFromCluster(String s)*/
  @Test
  public void testSplit1() {
    String s1 = "thrift://hcat1:port,thrift://hcat2:port;thrift://hcat3:port,thrift://hcat4:port;";
    p.put(EXTRA_HCAT_CLUSTERS, s1);
    List<String> s2 = Arrays.asList("thrift://hcat1:port,thrift://hcat2:port" , "thrift://hcat3:port,thrift://hcat4:port");
    Assert.assertTrue(p.getStringListFromCluster(EXTRA_HCAT_CLUSTERS).equals(s2));

    String s3 = "thrift://hcat1:port,thrift://hcat2:port     ;      thrift://hcat3:port,thrift://hcat4:port;";
    p.put(EXTRA_HCAT_CLUSTERS, s3);
    List<String> s4 = Arrays.asList( "thrift://hcat1:port,thrift://hcat2:port" , "thrift://hcat3:port,thrift://hcat4:port");
    Assert.assertTrue(p.getStringListFromCluster(EXTRA_HCAT_CLUSTERS).equals(s4));

    String s5 = "thrift://hcat1:port,thrift://hcat2:port";
    p.put(EXTRA_HCAT_CLUSTERS, s5);
    List<String> s6 = Arrays.asList("thrift://hcat1:port,thrift://hcat2:port");
    Assert.assertTrue(p.getStringListFromCluster(EXTRA_HCAT_CLUSTERS).equals(s6));
  }

  @Test
  public void testCreateProps() throws IOException {
    File file = Mockito.mock(File.class);
    Mockito.when(file.exists()).thenReturn(false);
    Props parent = new Props();
    Props props = new Props(parent, file);
    Assert.assertNull(props.getSource());
  }
}