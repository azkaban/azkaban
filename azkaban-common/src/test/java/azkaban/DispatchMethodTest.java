package azkaban;

import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_DISPATCH_MODEL;

public class DispatchMethodTest {
    @Test
    public void testDispatchMethod() {
        Props props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "CONTAINERIZED");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.CONTAINERIZED);
        props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "PUSH");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.PUSH);
        props = new Props();
        props.put(AZKABAN_DISPATCH_MODEL, "POLL");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.POLL);
    }
}

