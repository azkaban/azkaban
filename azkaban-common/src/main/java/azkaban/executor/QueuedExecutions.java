package azkaban.executor;

import azkaban.utils.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <pre>
 * Composite data structure to represent non-dispatched flows in webserver.
 * This data structure wraps a blocking queue and a concurrent hashmap.
 * </pre>
 */
public class QueuedExecutions {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedExecutions.class);
  final long capacity;

  /* map to easily access queued flows */
  final private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> queuedFlowMap;
  /* actual queue */
  final private BlockingQueue<Pair<ExecutionReference, ExecutableFlow>> queuedFlowList;

  public QueuedExecutions(final long capacity) {
    this.capacity = capacity;
    this.queuedFlowMap =
        new ConcurrentHashMap<>();
    this.queuedFlowList =
        new PriorityBlockingQueue<>(10,
            new ExecutableFlowPriorityComparator());
  }

  /**
   * Wraps BoundedQueue Take method to have a corresponding update in queuedFlowMap lookup table
   */
  public Pair<ExecutionReference, ExecutableFlow> fetchHead()
      throws InterruptedException {
    final Pair<ExecutionReference, ExecutableFlow> pair = this.queuedFlowList.take();
    if (pair != null && pair.getFirst() != null) {
      this.queuedFlowMap.remove(pair.getFirst().getExecId());
    }
    return pair;
  }

  /**
   * Helper method to have a single point of deletion in the queued flows
   */
  public void dequeue(final int executionId) {
    if (this.queuedFlowMap.containsKey(executionId)) {
      this.queuedFlowList.remove(this.queuedFlowMap.get(executionId));
      this.queuedFlowMap.remove(executionId);
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
  public void enqueue(final ExecutableFlow exflow, final ExecutionReference ref)
      throws ExecutorManagerException {
    if (hasExecution(exflow.getExecutionId())) {
      final String errMsg = "Flow already in queue " + exflow.getExecutionId();
      throw new ExecutorManagerException(errMsg);
    }

    final Pair<ExecutionReference, ExecutableFlow> pair =
        new Pair<>(ref, exflow);
    try {
      this.queuedFlowMap.put(exflow.getExecutionId(), pair);
      this.queuedFlowList.put(pair);
    } catch (final InterruptedException e) {
      final String errMsg = "Failed to insert flow " + exflow.getExecutionId();
      LOG.error(errMsg, e);
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
      final Collection<Pair<ExecutionReference, ExecutableFlow>> collection)
      throws ExecutorManagerException {
    for (final Pair<ExecutionReference, ExecutableFlow> pair : collection) {
      enqueue(pair.getSecond(), pair.getFirst());
    }
  }

  /**
   * Returns a read only collection of all the queued (flows, reference) pairs
   */
  public Collection<Pair<ExecutionReference, ExecutableFlow>> getAllEntries() {
    return Collections.unmodifiableCollection(this.queuedFlowMap.values());
  }

  /**
   * Checks if an execution is queued or not
   */
  public boolean hasExecution(final int executionId) {
    return this.queuedFlowMap.containsKey(executionId);
  }

  /**
   * Fetch flow for an execution. Returns null, if execution not in queue
   */
  public ExecutableFlow getFlow(final int executionId) {
    if (hasExecution(executionId)) {
      return this.queuedFlowMap.get(executionId).getSecond();
    }
    return null;
  }

  /**
   * Fetch Activereference for an execution. Returns null, if execution not in queue
   */
  public ExecutionReference getReference(final int executionId) {
    if (hasExecution(executionId)) {
      return this.queuedFlowMap.get(executionId).getFirst();
    }
    return null;
  }

  /**
   * Size of the queue
   */
  public long size() {
    return this.queuedFlowList.size();
  }

  /**
   * Verify, if queue is full as per initialized capacity
   */
  public boolean isFull() {
    return size() >= this.capacity;
  }

  /**
   * Verify, if queue is empty or not
   */
  public boolean isEmpty() {
    return this.queuedFlowList.isEmpty() && this.queuedFlowMap.isEmpty();
  }

  /**
   * Empties queue by dequeuing all the elements
   */
  public void clear() {
    for (final Pair<ExecutionReference, ExecutableFlow> pair : this.queuedFlowMap.values()) {
      dequeue(pair.getFirst().getExecId());
    }
  }
}
