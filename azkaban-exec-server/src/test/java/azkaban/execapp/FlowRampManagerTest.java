package azkaban.execapp;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FlowRampManagerTest {

    private FlowRampManager.RampDataModel rampDataModel;

    @Before
    public void setup() throws Exception {
        this.rampDataModel = new FlowRampManager.RampDataModel();
    }

    @Test
    public void test() {
        Assert.assertFalse(rampDataModel.hasUnsavedFinishedFlow());
        Assert.assertEquals(0, rampDataModel.getBeginFlowCount());
        Assert.assertEquals(0, rampDataModel.getEndFlowCount());

        rampDataModel.beginFlow(1, ImmutableSet.<String>builder().add("daliSpark").build());
        rampDataModel.beginFlow(2, ImmutableSet.<String>builder().add("daliSpark").build());
        rampDataModel.endFlow(1);
        rampDataModel.beginFlow(3, ImmutableSet.<String>builder().add("daliSpark").build());
        rampDataModel.beginFlow(4, ImmutableSet.<String>builder().add("daliSpark").build());
        rampDataModel.endFlow(3);
        Assert.assertTrue(rampDataModel.hasUnsavedFinishedFlow());
        Assert.assertEquals(4, rampDataModel.getBeginFlowCount());
        Assert.assertEquals(2, rampDataModel.getEndFlowCount());

        Map<Integer, Set<String>> executingFlows = rampDataModel.getExecutingFlows();
        Assert.assertEquals(2, executingFlows.size());
        Assert.assertFalse(executingFlows.containsKey(1));
        Assert.assertTrue(executingFlows.containsKey(2));
        Assert.assertFalse(executingFlows.containsKey(3));
        Assert.assertTrue(executingFlows.containsKey(4));

        rampDataModel.resetFlowCountAfterSave();
        Assert.assertFalse(rampDataModel.hasUnsavedFinishedFlow());
        Assert.assertEquals(2, rampDataModel.getBeginFlowCount());
        Assert.assertEquals(0, rampDataModel.getEndFlowCount());

        executingFlows = rampDataModel.getExecutingFlows();
        Assert.assertEquals(2, executingFlows.size());
        Assert.assertFalse(executingFlows.containsKey(1));
        Assert.assertTrue(executingFlows.containsKey(2));
        Assert.assertFalse(executingFlows.containsKey(3));
        Assert.assertTrue(executingFlows.containsKey(4));
    }
}
