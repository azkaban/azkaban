package azkaban;

import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD;

public class DispatchMethodTest {
    @Test
    public void testDispatchMethod() {
        Props props = new Props();
        props.put(AZKABAN_EXECUTION_DISPATCH_METHOD, "CONTAINERIZED");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.CONTAINERIZED);
        props = new Props();
        props.put(AZKABAN_EXECUTION_DISPATCH_METHOD, "PUSH");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.PUSH);
        props = new Props();
        props.put(AZKABAN_EXECUTION_DISPATCH_METHOD, "POLL");
        Assert.assertEquals(DispatchMethod.getDispatchModel(props), DispatchMethod.POLL);
    }
}

