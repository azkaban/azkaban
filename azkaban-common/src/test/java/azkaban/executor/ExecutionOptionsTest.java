package azkaban.executor;

import org.junit.Assert;
import org.junit.Test;

public class ExecutionOptionsTest {

    @Test
    public void testUseExecutorById() {
        Assert.assertEquals(Integer.valueOf(1), ExecutionOptions.useExecutorById("1"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorById("abc"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorById(""));
        Assert.assertEquals(null, ExecutionOptions.useExecutorById("  "));
    }

    @Test
    public void testUseExecutorByHostPort() {
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("1"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort(" "));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("noSemiColon"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("hostOnly: "));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("hostOnly:"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort(" :9090"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort(":9090"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("host:stringPort"));
        Assert.assertEquals(null, ExecutionOptions.useExecutorByHostPort("host:-1"));
        Assert.assertEquals(new ExecutionOptions.HostPort("host", 9090), ExecutionOptions.useExecutorByHostPort("host:9090"));
    }
}
