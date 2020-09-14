package azkaban;

import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_DISPATCH_MODEL;

public class DispatchMethodTest {
    @Test
    public void testDispatchMethod() {
        Props props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "containerized");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.CONTAINERIZED);
        props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "push");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.PUSH);
        props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "poll");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.POLL);
    }
}

