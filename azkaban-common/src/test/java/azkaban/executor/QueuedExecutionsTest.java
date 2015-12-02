package azkaban.executor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.TestUtils;

public class QueuedExecutionsTest {

  private File getFlowDir(String flow) {
    return TestUtils.getFlowDir("exectest1", flow);
  }

  /*
   * Helper method to create an (ExecutionReference, ExecutableFlow) from
   * serialized description
   */
  private Pair<ExecutionReference, ExecutableFlow> createExecutablePair(
    String flowName, int execId) throws IOException {
    File jsonFlowFile = getFlowDir(flowName);
    @SuppressWarnings("unchecked")
    HashMap<String, Object> flowObj =
      (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    Flow flow = Flow.flowFromObject(flowObj);
    Project project = new Project(1, "flow");
    HashMap<String, Flow> flowMap = new HashMap<String, Flow>();
    flowMap.put(flow.getId(), flow);
    project.setFlows(flowMap);
    ExecutableFlow execFlow = new ExecutableFlow(project, flow);
    execFlow.setExecutionId(execId);
    ExecutionReference ref = new ExecutionReference(execId);
    return new Pair<ExecutionReference, ExecutableFlow>(ref, execFlow);
  }

  public List<Pair<ExecutionReference, ExecutableFlow>> getDummyData()
    throws IOException {
    List<Pair<ExecutionReference, ExecutableFlow>> dataList =
      new ArrayList<Pair<ExecutionReference, ExecutableFlow>>();
    dataList.add(createExecutablePair("exec1", 1));
    dataList.add(createExecutablePair("exec2", 2));
    return dataList;
  }

  /* Test enqueue method happy case */
  @Test
  public void testEnqueueHappyCase() throws IOException,
    ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    for (Pair<ExecutionReference, ExecutableFlow> pair : dataList) {
      queue.enqueue(pair.getSecond(), pair.getFirst());
    }

    Assert.assertTrue(queue.getAllEntries().containsAll(dataList));
    Assert.assertTrue(dataList.containsAll(queue.getAllEntries()));
  }

  /* Test enqueue duplicate execution ids */
  @Test(expected = ExecutorManagerException.class)
  public void testEnqueueDuplicateExecution() throws IOException,
    ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair1 =
      createExecutablePair("exec1", 1);
    QueuedExecutions queue = new QueuedExecutions(5);
    queue.enqueue(pair1.getSecond(), pair1.getFirst());
    queue.enqueue(pair1.getSecond(), pair1.getFirst());
  }

  /* Test enqueue more than capacity */
  @Test(expected = ExecutorManagerException.class)
  public void testEnqueueOverflow() throws IOException,
    ExecutorManagerException {
    Pair<ExecutionReference, ExecutableFlow> pair1 =
      createExecutablePair("exec1", 1);
    QueuedExecutions queue = new QueuedExecutions(1);
    queue.enqueue(pair1.getSecond(), pair1.getFirst());
    queue.enqueue(pair1.getSecond(), pair1.getFirst());
  }

  /* Test EnqueueAll method */
  @Test
  public void testEnqueueAll() throws IOException, ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    Assert.assertTrue(queue.getAllEntries().containsAll(dataList));
    Assert.assertTrue(dataList.containsAll(queue.getAllEntries()));
  }

  /* Test size method */
  @Test
  public void testSize() throws IOException, ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    Assert.assertEquals(queue.size(), 2);
  }

  /* Test dequeue method */
  @Test
  public void testDequeue() throws IOException, ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    queue.dequeue(dataList.get(0).getFirst().getExecId());
    Assert.assertEquals(queue.size(), 1);
    Assert.assertTrue(queue.getAllEntries().contains(dataList.get(1)));
  }

  /* Test clear method */
  @Test
  public void testClear() throws IOException, ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    Assert.assertEquals(queue.size(), 2);
    queue.clear();
    Assert.assertEquals(queue.size(), 0);
  }

  /* Test isEmpty method */
  @Test
  public void testIsEmpty() throws IOException, ExecutorManagerException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    Assert.assertTrue(queue.isEmpty());
    queue.enqueueAll(dataList);
    Assert.assertEquals(queue.size(), 2);
    queue.clear();
    Assert.assertTrue(queue.isEmpty());
  }

  /* Test fetchHead method */
  @Test
  public void testFetchHead() throws IOException, ExecutorManagerException,
    InterruptedException {
    QueuedExecutions queue = new QueuedExecutions(5);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    Assert.assertTrue(queue.isEmpty());
    queue.enqueueAll(dataList);
    Assert.assertEquals(queue.fetchHead(), dataList.get(0));
    Assert.assertEquals(queue.fetchHead(), dataList.get(1));
  }

  /* Test isFull method */
  @Test
  public void testIsFull() throws IOException, ExecutorManagerException,
    InterruptedException {
    QueuedExecutions queue = new QueuedExecutions(2);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    Assert.assertTrue(queue.isFull());
  }

  /* Test hasExecution method */
  @Test
  public void testHasExecution() throws IOException, ExecutorManagerException,
    InterruptedException {
    QueuedExecutions queue = new QueuedExecutions(2);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    for (Pair<ExecutionReference, ExecutableFlow> pair : dataList) {
      Assert.assertTrue(queue.hasExecution(pair.getFirst().getExecId()));
    }
    Assert.assertFalse(queue.hasExecution(5));
    Assert.assertFalse(queue.hasExecution(7));
    Assert.assertFalse(queue.hasExecution(15));
  }

  /* Test getFlow method */
  @Test
  public void testGetFlow() throws IOException, ExecutorManagerException,
    InterruptedException {
    QueuedExecutions queue = new QueuedExecutions(2);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    for (Pair<ExecutionReference, ExecutableFlow> pair : dataList) {
      Assert.assertEquals(pair.getSecond(),
        queue.getFlow(pair.getFirst().getExecId()));
    }
  }

  /* Test getReferences method */
  @Test
  public void testGetReferences() throws IOException, ExecutorManagerException,
    InterruptedException {
    QueuedExecutions queue = new QueuedExecutions(2);
    List<Pair<ExecutionReference, ExecutableFlow>> dataList = getDummyData();
    queue.enqueueAll(dataList);
    for (Pair<ExecutionReference, ExecutableFlow> pair : dataList) {
      Assert.assertEquals(pair.getFirst(),
        queue.getReference(pair.getFirst().getExecId()));
    }
  }
}
