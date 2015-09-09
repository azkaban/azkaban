package azkaban.executor;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import azkaban.utils.Pair;

/**
 * <pre>
 * Composite data structure to represent non-dispatched flows in webserver.
 * This data structure wraps a blocking queue and a concurrent hashmap.
 * </pre>
 */
public class QueuedExecutions {
  private static Logger logger = Logger.getLogger(QueuedExecutions.class);
  final long capacity;

  /* map to easily access queued flows */
  final private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> queuedFlowMap;
  /* actual queue */
  final private BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queuedFlowList;

  public QueuedExecutions(long capacity) {
    this.capacity = capacity;
    queuedFlowMap =
      new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
    queuedFlowList =
      new PriorityBlockingQueue<Pair<ExecutionReference, ExecutableFlow>>(10,
        new ExecutableFlowPriorityComparator());
  }

  /**
   * Wraps BoundedQueue Take method to have a corresponding update in
   * queuedFlowMap lookup table
   *
   * @return
   * @throws InterruptedException
   */
  public Pair<ExecutionReference, ExecutableFlow> fetchHead()
    throws InterruptedException {
    Pair<ExecutionReference, ExecutableFlow> pair = queuedFlowList.take();
    if (pair != null && pair.getFirst() != null) {
      queuedFlowMap.remove(pair.getFirst().getExecId());
    }
    return pair;
  }

  /**
   * Helper method to have a single point of deletion in the queued flows
   *
   * @param executionId
   */
  public void dequeue(int executionId) {
    if (queuedFlowMap.containsKey(executionId)) {
      queuedFlowList.remove(queuedFlowMap.get(executionId));
      queuedFlowMap.remove(executionId);
    }
  }

  /**
   * <pre>
   * Helper method to have a single point of insertion in the queued flows
   *
   * @param exflow
   *          flow to be enqueued
   * @param ref
   *          reference to be enqueued
   * @throws ExecutorManagerException
   *           case 1: if blocking queue put method fails due to
   *           InterruptedException
   *           case 2: if there already an element with
   *           same execution Id
   * </pre>
   */
  public void enqueue(ExecutableFlow exflow, ExecutionReference ref)
    throws ExecutorManagerException {
    if (hasExecution(exflow.getExecutionId())) {
      String errMsg = "Flow already in queue " + exflow.getExecutionId();
      throw new ExecutorManagerException(errMsg);
    }

    Pair<ExecutionReference, ExecutableFlow> pair =
      new Pair<ExecutionReference, ExecutableFlow>(ref, exflow);
    try {
      queuedFlowMap.put(exflow.getExecutionId(), pair);
      queuedFlowList.put(pair);
    } catch (InterruptedException e) {
      String errMsg = "Failed to insert flow " + exflow.getExecutionId();
      logger.error(errMsg, e);
      throw new ExecutorManagerException(errMsg);
    }
  }

  /**
   * <pre>
   * Enqueues all the elements of a collection
   *
   * @param collection
   *
   * @throws ExecutorManagerException
   *           case 1: if blocking queue put method fails due to
   *           InterruptedException
   *           case 2: if there already an element with
   *           same execution Id
   * </pre>
   */
  public void enqueueAll(
    Collection<Pair<ExecutionReference, ExecutableFlow>> collection)
    throws ExecutorManagerException {
    for (Pair<ExecutionReference, ExecutableFlow> pair : collection) {
      enqueue(pair.getSecond(), pair.getFirst());
    }
  }

  /**
   * Returns a read only collection of all the queued (flows, reference) pairs
   *
   * @return
   */
  public Collection<Pair<ExecutionReference, ExecutableFlow>> getAllEntries() {
    return Collections.unmodifiableCollection(queuedFlowMap.values());
  }

  /**
   * Checks if an execution is queued or not
   *
   * @param executionId
   * @return
   */
  public boolean hasExecution(int executionId) {
    return queuedFlowMap.containsKey(executionId);
  }

  /**
   * Fetch flow for an execution. Returns null, if execution not in queue
   *
   * @param executionId
   * @return
   */
  public ExecutableFlow getFlow(int executionId) {
    if (hasExecution(executionId)) {
      return queuedFlowMap.get(executionId).getSecond();
    }
    return null;
  }

  /**
   * Fetch Activereference for an execution. Returns null, if execution not in
   * queue
   *
   * @param executionId
   * @return
   */
  public ExecutionReference getReference(int executionId) {
    if (hasExecution(executionId)) {
      return queuedFlowMap.get(executionId).getFirst();
    }
    return null;
  }

  /**
   * Size of the queue
   *
   * @return
   */
  public long size() {
    return queuedFlowList.size();
  }

  /**
   * Verify, if queue is full as per initialized capacity
   *
   * @return
   */
  public boolean isFull() {
    return size() >= capacity;
  }

  /**
   * Verify, if queue is empty or not
   *
   * @return
   */
  public boolean isEmpty() {
    return queuedFlowList.isEmpty() && queuedFlowMap.isEmpty();
  }

  /**
   * Empties queue by dequeuing all the elements
   */
  public void clear() {
    for (Pair<ExecutionReference, ExecutableFlow> pair : queuedFlowMap.values()) {
      dequeue(pair.getFirst().getExecId());
    }
  }
}
