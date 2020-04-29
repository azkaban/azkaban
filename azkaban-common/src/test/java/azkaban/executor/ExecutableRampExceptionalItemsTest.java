package azkaban.executor;

import java.util.ArrayList;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ExecutableRampExceptionalItemsTest {
  private static final String RAMP_ID1 = "daliSpark";
  private static final String FLOW_NAME1 = "project1.flow1";
  private static final String FLOW_NAME2 = "project1.flow2";

  private ExecutableRampExceptionalItems items;
  private long timeStamp = 0L;

  @Before
  public void setup() throws Exception {
    items = ExecutableRampExceptionalItems.createInstance();
    timeStamp = System.currentTimeMillis();
  }

  @Test
  public void testEmptySet() {
    Assert.assertTrue(items.getItems().isEmpty());
  }

  @Test
  public void testAddItem() {
    items.add(FLOW_NAME1, ExecutableRampStatus.BLACKLISTED, timeStamp, true);
    items.add(FLOW_NAME2, ExecutableRampStatus.WHITELISTED, timeStamp, false);
    Assert.assertEquals(2, items.elementCount());
  }

  @Test
  public void testCache() {
    items.add(FLOW_NAME1, ExecutableRampStatus.BLACKLISTED, timeStamp, true);
    items.add(FLOW_NAME2, ExecutableRampStatus.WHITELISTED, timeStamp, false);
    Assert.assertEquals(1, items.getCachedItems().size());

    Object[][] parameters = items.getCachedItems().stream()
        .map(item -> {
          ArrayList<Object> object = new ArrayList<>();
          object.add(RAMP_ID1);
          object.add(item.getKey());
          object.add(item.getValue().getStatus().getKey());
          object.add(item.getValue().getTimeStamp());
          return object.toArray();
        })
        .collect(Collectors.toList()).toArray(new Object[0][]);
    Assert.assertEquals(1, parameters.length);
    Assert.assertEquals(RAMP_ID1, parameters[0][0]);
    Assert.assertEquals(FLOW_NAME1, parameters[0][1]);
    Assert.assertEquals(ExecutableRampStatus.BLACKLISTED.getKey(), parameters[0][2]);
    Assert.assertEquals(timeStamp, parameters[0][3]);

    items.resetCacheFlag();
    Assert.assertEquals(0, items.getCachedItems().size());

    parameters = items.getCachedItems().stream()
        .map(item -> {
          ArrayList<Object> object = new ArrayList<>();
          object.add(RAMP_ID1);
          object.add(item.getKey());
          object.add(item.getValue().getStatus().getKey());
          object.add(item.getValue().getTimeStamp());
          return object.toArray();
        })
        .collect(Collectors.toList()).toArray(new Object[0][]);
    Assert.assertEquals(0, parameters.length);
  }
}
