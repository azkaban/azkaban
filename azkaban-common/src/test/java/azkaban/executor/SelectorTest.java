/*
 * Copyright 2015 LinkedIn Corp.
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

package azkaban.executor;

import azkaban.executor.selector.CandidateComparator;
import azkaban.executor.selector.CandidateFilter;
import azkaban.executor.selector.CandidateSelector;
import azkaban.executor.selector.ExecutorComparator;
import azkaban.executor.selector.ExecutorFilter;
import azkaban.executor.selector.ExecutorSelector;
import azkaban.executor.selector.FactorComparator;
import azkaban.executor.selector.FactorFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectorTest {

  // test samples.
  protected ArrayList<MockExecutorObject> executorList = new ArrayList<>();

  @BeforeClass
  public static void onlyOnce() {
    BasicConfigurator.configure();
  }

  @Before
  public void setUp() throws Exception {
    this.executorList.clear();
    this.executorList
        .add(new MockExecutorObject("Executor1", 8080, 50.0, 2048, 5, new Date(), 20, 6400));
    this.executorList
        .add(new MockExecutorObject("Executor2", 8080, 50.0, 2048, 4, new Date(), 20, 6400));
    this.executorList
        .add(new MockExecutorObject("Executor3", 8080, 40.0, 2048, 1, new Date(), 20, 6400));
    this.executorList
        .add(new MockExecutorObject("Executor4", 8080, 50.0, 2048, 4, new Date(), 20, 6400));
    this.executorList
        .add(new MockExecutorObject("Executor5", 8080, 50.0, 1024, 5, new Date(), 90, 6400));
    this.executorList
        .add(new MockExecutorObject("Executor6", 8080, 50.0, 1024, 5, new Date(), 90, 3200));
    this.executorList
        .add(new MockExecutorObject("Executor7", 8080, 50.0, 1024, 5, new Date(), 90, 3200));
    this.executorList
        .add(new MockExecutorObject("Executor8", 8080, 50.0, 2048, 1, new Date(), 90, 3200));
    this.executorList
        .add(new MockExecutorObject("Executor9", 8080, 50.0, 2050, 5, new Date(), 90, 4200));
    this.executorList
        .add(new MockExecutorObject("Executor10", 8080, 00.0, 1024, 1, new Date(), 90, 3200));
    this.executorList
        .add(new MockExecutorObject("Executor11", 8080, 20.0, 2096, 3, new Date(), 90, 2400));
    this.executorList
        .add(new MockExecutorObject("Executor12", 8080, 90.0, 2050, 5, new Date(), 60, 2500));

    // make sure each time the order is different.
    Collections.shuffle(this.executorList);
  }

  private MockExecutorObject getExecutorByName(final String name) {
    MockExecutorObject returnVal = null;
    for (final MockExecutorObject item : this.executorList) {
      if (item.name.equals(name)) {
        returnVal = item;
        break;
      }
    }
    return returnVal;
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testExecutorFilter() throws Exception {

    // mock object, remaining memory 11500, total memory 3095, remainingTmpSpace 4200, priority 2.
    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 3096, 1500, 4200, 2);

    final MockFilter mFilter = new MockFilter();
    mFilter.registerFilterforRemainingMemory();

    // expect true.
    boolean result = mFilter.filterTarget(this.getExecutorByName("Executor1"), dispatchingObj);
    Assert.assertTrue(result);

    //expect true.
    result = mFilter.filterTarget(this.getExecutorByName("Executor3"), dispatchingObj);
      /*
      1 [main] INFO azkaban.executor.Selector.CandidateFilter  - start checking 'Executor3' with factor filter for 'Mockfilter'
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - Final checking result : true
      */
    Assert.assertTrue(result);

    // add the priority filter.
    mFilter.registerFilterforPriority();
    result = mFilter.filterTarget(this.getExecutorByName("Executor3"), dispatchingObj);
    // expect false, for priority.
      /*
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - start checking 'Executor3' with factor filter for 'Mockfilter'
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredProprity] filter result : false
      2 [main] INFO azkaban.executor.Selector.CandidateFilter  - Final checking result : false
      */
    Assert.assertFalse(result);

    // add the remaining space filter.
    mFilter.registerFilterforRemainingTmpSpace();

    // expect pass.
    result = mFilter.filterTarget(this.getExecutorByName("Executor2"), dispatchingObj);
      /*
      3 [main] INFO azkaban.executor.Selector.CandidateFilter  - start checking 'Executor2' with factor filter for 'Mockfilter'
      3 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      3 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingTmpSpace] filter result : true
      3 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredProprity] filter result : true
      3 [main] INFO azkaban.executor.Selector.CandidateFilter  - Final checking result : true
      */
    Assert.assertTrue(result);

    // expect false, remaining tmp, priority will also fail but the logic shortcuts when the Tmp size check Fails.
    result = mFilter.filterTarget(this.getExecutorByName("Executor8"), dispatchingObj);
      /*
      4 [main] INFO azkaban.executor.Selector.CandidateFilter  - start checking 'Executor8' with factor filter for 'Mockfilter'
      4 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      4 [main] INFO azkaban.executor.Selector.CandidateFilter  - [Factor: requiredRemainingTmpSpace] filter result : false
      4 [main] INFO azkaban.executor.Selector.CandidateFilter  - Final checking result : false
      */
    Assert.assertFalse(result);

  }

  @Test
  public void testExecutorFilterWithNullInputs() throws Exception {
    final MockFilter filter = new MockFilter();
    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();
    boolean result = false;
    try {
      result = filter.filterTarget(this.getExecutorByName("Executor1"), null);
    } catch (final Exception ex) {
      Assert.fail("no exception should be thrown when null value is passed to the filter.");
    }
    // note : the FactorFilter logic will decide whether true or false should be returned when null value
    //        is passed, for the Mock class it returns false.
    Assert.assertFalse(result);

    try {
      result = filter.filterTarget(null, null);
    } catch (final Exception ex) {
      Assert.fail("no exception should be thrown when null value is passed to the filter.");
    }
    // note : the FactorFilter logic will decide whether true or false should be returned when null value
    //        is passed, for the Mock class it returns false.
    Assert.assertFalse(result);
  }

  @Test
  public void testExecutorComparer() throws Exception {
    final MockComparator comparator = new MockComparator();
    comparator.registerComparerForMemory(5);

    MockExecutorObject nextExecutor = Collections.max(this.executorList, comparator);

    // expect the first item to be selected, memory wise it is the max.
    Assert.assertEquals(this.getExecutorByName("Executor11"), nextExecutor);

    // add the priority factor.
    // expect again the #9 item to be selected.
    comparator.registerComparerForPriority(6);
    nextExecutor = Collections.max(this.executorList, comparator);
    Assert.assertEquals(this.getExecutorByName("Executor12"), nextExecutor);

    // add the remaining space factor.
    // expect the #12 item to be returned.
    comparator.registerComparerForRemainingSpace(3);
    nextExecutor = Collections.max(this.executorList, comparator);
    Assert.assertEquals(this.getExecutorByName("Executor12"), nextExecutor);
  }

  @Test
  public void testExecutorComparerResisterComparerWInvalidWeight() throws Exception {
    final MockComparator comparator = new MockComparator();
    comparator.registerComparerForMemory(0);
  }

  @Test
  public void testSelector() throws Exception {
    final MockFilter filter = new MockFilter();
    final MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(5);
    comparator.registerComparerForRemainingSpace(3);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        filter, comparator);

    // mock object, remaining memory 11500, total memory 3095, remainingTmpSpace 4200, priority 2.
    MockFlowObject dispatchingObj = new MockFlowObject("flow1", 3096, 1500, 4200, 2);

    // expected selection = #12
    MockExecutorObject nextExecutor = morkSelector.getBest(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor1"), nextExecutor);

    // remaining memory 11500, total memory 3095, remainingTmpSpace 14200, priority 2.
    dispatchingObj = new MockFlowObject("flow1", 3096, 1500, 14200, 2);
    // all candidates should be filtered by the remaining memory.
    nextExecutor = morkSelector.getBest(this.executorList, dispatchingObj);
    Assert.assertEquals(null, nextExecutor);
  }

  @Test
  public void testSelectorsignleCandidate() throws Exception {
    final MockFilter filter = new MockFilter();
    final MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        filter, comparator);

    final ArrayList<MockExecutorObject> signleExecutorList = new ArrayList<>();
    final MockExecutorObject signleExecutor = new MockExecutorObject("ExecutorX", 8080, 50.0, 2048,
        3,
        new Date(), 20, 6400);
    signleExecutorList.add(signleExecutor);

    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 100, 100, 100, 5);
    MockExecutorObject executor = morkSelector.getBest(signleExecutorList, dispatchingObj);
    // expected to see null result, as the only executor is filtered out .
    Assert.assertTrue(null == executor);

    // adjust the priority to let the executor pass the filter.
    dispatchingObj.priority = 3;
    executor = morkSelector.getBest(signleExecutorList, dispatchingObj);
    Assert.assertEquals(signleExecutor, executor);
  }

  @Test
  public void testSelectorListWithItemsThatAreReferenceEqual() throws Exception {
    final MockFilter filter = new MockFilter();
    final MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        filter, comparator);

    final ArrayList<MockExecutorObject> list = new ArrayList<>();
    final MockExecutorObject signleExecutor = new MockExecutorObject("ExecutorX", 8080, 50.0, 2048,
        3,
        new Date(), 20, 6400);
    list.add(signleExecutor);
    list.add(signleExecutor);
    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 100, 100, 100, 3);
    final MockExecutorObject executor = morkSelector.getBest(list, dispatchingObj);
    Assert.assertTrue(signleExecutor == executor);
  }

  @Test
  public void testSelectorListWithItemsThatAreEqualInValue() throws Exception {
    final MockFilter filter = new MockFilter();
    final MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        filter, comparator);

    // note - as the tieBreaker set in the MockComparator uses the name value of the executor to do the
    //        final diff therefore we need to set the name differently to make a meaningful test, in real
    //        scenario we may want to use something else (say hash code) to be the bottom line for the tieBreaker
    //        to make a final decision, the purpose of the test here is to prove that for two candidates with
    //        exact value (in the case of test, all values except for the name) the decision result is stable.
    final ArrayList<MockExecutorObject> list = new ArrayList<>();
    final MockExecutorObject executor1 = new MockExecutorObject("ExecutorX", 8080, 50.0, 2048, 3,
        new Date(), 20, 6400);
    final MockExecutorObject executor2 = new MockExecutorObject("ExecutorX2", 8080, 50.0, 2048, 3,
        new Date(), 20, 6400);
    list.add(executor1);
    list.add(executor2);
    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 100, 100, 100, 3);
    MockExecutorObject executor = morkSelector.getBest(list, dispatchingObj);
    Assert.assertTrue(executor2 == executor);

    // shuffle and test again.
    list.remove(0);
    list.add(executor1);
    executor = morkSelector.getBest(list, dispatchingObj);
    Assert.assertTrue(executor2 == executor);
  }

  @Test
  public void testSelectorEmptyList() throws Exception {
    final MockFilter filter = new MockFilter();
    final MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        filter, comparator);

    final ArrayList<MockExecutorObject> list = new ArrayList<>();

    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 100, 100, 100, 5);

    MockExecutorObject executor = null;

    try {
      executor = morkSelector.getBest(list, dispatchingObj);
    } catch (final Exception ex) {
      Assert.fail("no exception should be thrown when an empty list is passed to the Selector.");
    }

    // expected to see null result.
    Assert.assertTrue(null == executor);

    try {
      executor = morkSelector.getBest(list, dispatchingObj);
    } catch (final Exception ex) {
      Assert.fail(
          "no exception should be thrown when null is passed to the Selector as the candidate list.");
    }

    // expected to see null result, as the only executor is filtered out .
    Assert.assertTrue(null == executor);

  }

  @Test
  public void testSelectorListWithNullValue() throws Exception {
    final MockComparator comparator = new MockComparator();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    final CandidateSelector<MockExecutorObject, MockFlowObject> morkSelector = new CandidateSelector<>(
        null, comparator);

    final ArrayList<MockExecutorObject> list = new ArrayList<>();
    final MockExecutorObject executor1 = new MockExecutorObject("ExecutorX", 8080, 50.0, 2048, 3,
        new Date(), 20, 6400);
    final MockExecutorObject executor2 = new MockExecutorObject("ExecutorX2", 8080, 50.0, 2048, 3,
        new Date(), 20, 6400);
    list.add(executor1);
    list.add(executor2);
    list.add(null);

    final MockFlowObject dispatchingObj = new MockFlowObject("flow1", 100, 100, 100, 3);
    MockExecutorObject executor = null;
    try {
      executor = morkSelector.getBest(list, dispatchingObj);
    } catch (final Exception ex) {
      Assert.fail("no exception should be thrown when an List contains null value.");
    }
    Assert.assertTrue(executor2 == executor);

    // try to compare null vs null, no exception is expected.
    list.clear();
    list.add(null);
    list.add(null);
    try {
      executor = morkSelector.getBest(list, dispatchingObj);
    } catch (final Exception ex) {
      Assert.fail("no exception should be thrown when an List contains multiple null values.");
    }
    Assert.assertTrue(null == executor);

  }

  @Test
  public void testCreatingExectorfilterObject() throws Exception {
    final List<String> validList = new ArrayList<>(ExecutorFilter.getAvailableFilterNames());
    try {
      new ExecutorFilter(validList);
    } catch (final Exception ex) {
      Assert.fail(
          "creating ExecutorFilter with valid list throws exception . ex -" + ex.getMessage());
    }
  }

  @Test
  public void testCreatingExectorfilterObjectWInvalidList() throws Exception {
    final List<String> invalidList = new ArrayList<>();
    invalidList.add("notExistingFilter");
    Exception result = null;
    try {
      new ExecutorFilter(invalidList);
    } catch (final Exception ex) {
      if (ex instanceof IllegalArgumentException) {
        result = ex;
      }
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExectorComparatorObject() throws Exception {
    final Map<String, Integer> comparatorMap = new HashMap<>();
    for (final String name : ExecutorComparator.getAvailableComparatorNames()) {
      comparatorMap.put(name, 1);
    }
    try {
      new ExecutorComparator(comparatorMap);
    } catch (final Exception ex) {
      Assert.fail(
          "creating ExecutorComparator with valid list throws exception . ex -" + ex.getMessage());
    }
  }

  @Test
  public void testCreatingExectorComparatorObjectWInvalidName() throws Exception {
    final Map<String, Integer> comparatorMap = new HashMap<>();
    comparatorMap.put("invalidName", 0);
    Exception result = null;
    try {
      new ExecutorComparator(comparatorMap);
    } catch (final Exception ex) {
      if (ex instanceof IllegalArgumentException) {
        result = ex;
      }
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExectorComparatorObjectWInvalidWeight() throws Exception {
    final Map<String, Integer> comparatorMap = new HashMap<>();
    for (final String name : ExecutorComparator.getAvailableComparatorNames()) {
      comparatorMap.put(name, -1);
    }
    Exception result = null;
    try {
      new ExecutorComparator(comparatorMap);
    } catch (final Exception ex) {
      if (ex instanceof IllegalArgumentException) {
        result = ex;
      }
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExecutorSelectorWithEmptyFilterComparatorList() throws Exception {
    final List<Executor> executorList = new ArrayList<>();
    executorList.add(new Executor(1, "host1", 80, true, null));
    executorList.add(new Executor(2, "host2", 80, true, null));
    executorList.add(new Executor(3, "host3", 80, true, null));

    executorList.get(0)
        .setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89, 0));
    executorList.get(1)
        .setExecutorInfo(new ExecutorInfo(50, 14095, 50, System.currentTimeMillis(), 90, 0));
    executorList.get(2)
        .setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 90, 0));

    final ExecutableFlow flow = new ExecutableFlow();

    final ExecutorSelector selector = new ExecutorSelector(null, null);
    final Executor executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(2), executor);
  }

  @Test
  public void testExecutorSelectorE2E() throws Exception {
    final List<String> filterList = new ArrayList<>(ExecutorFilter.getAvailableFilterNames());
    final Map<String, Integer> comparatorMap;
    comparatorMap = new HashMap<>();
    final List<Executor> executorList = new ArrayList<>();
    executorList.add(new Executor(1, "host1", 80, true, null));
    executorList.add(new Executor(2, "host2", 80, true, null));
    executorList.add(new Executor(3, "host3", 80, true, null));

    executorList.get(0)
        .setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89, 0));
    executorList.get(1)
        .setExecutorInfo(new ExecutorInfo(50, 14095, 50, System.currentTimeMillis(), 90, 0));
    executorList.get(2)
        .setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 90, 0));

    final ExecutableFlow flow = new ExecutableFlow();

    for (final String name : ExecutorComparator.getAvailableComparatorNames()) {
      comparatorMap.put(name, 1);
    }
    final ExecutorSelector selector = new ExecutorSelector(filterList, comparatorMap);
    Executor executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(0), executor);

    // simulate that once the flow is assigned, executor1's remaining TMP storage dropped to 2048
    // now we do the getBest again executor3 is expected to be selected as it has a earlier last dispatched time.
    executorList.get(0)
        .setExecutorInfo(new ExecutorInfo(99.9, 4095, 50, System.currentTimeMillis(), 90, 1));
    executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(2), executor);
  }

  // mock executor object.
  static class MockExecutorObject implements Comparable<MockExecutorObject> {

    public String name;
    public int port;
    public double percentOfRemainingMemory;
    public int amountOfRemainingMemory;
    public int priority;
    public Date lastAssigned;
    public double percentOfRemainingFlowcapacity;
    public int remainingTmp;

    public MockExecutorObject(final String name,
        final int port,
        final double percentOfRemainingMemory,
        final int amountOfRemainingMemory,
        final int priority,
        final Date lastAssigned,
        final double percentOfRemainingFlowcapacity,
        final int remainingTmp) {
      this.name = name;
      this.port = port;
      this.percentOfRemainingMemory = percentOfRemainingMemory;
      this.amountOfRemainingMemory = amountOfRemainingMemory;
      this.priority = priority;
      this.lastAssigned = lastAssigned;
      this.percentOfRemainingFlowcapacity = percentOfRemainingFlowcapacity;
      this.remainingTmp = remainingTmp;
    }

    @Override
    public String toString() {
      return this.name;
    }

    @Override
    public int compareTo(final MockExecutorObject o) {
      return null == o ? 1 : this.hashCode() - o.hashCode();
    }
  }

  // Mock flow object.
  static class MockFlowObject {

    public String name;
    public int requiredRemainingMemory;
    public int requiredTotalMemory;
    public int requiredRemainingTmpSpace;
    public int priority;

    public MockFlowObject(final String name,
        final int requiredTotalMemory,
        final int requiredRemainingMemory,
        final int requiredRemainingTmpSpace,
        final int priority) {
      this.name = name;
      this.requiredTotalMemory = requiredTotalMemory;
      this.requiredRemainingMemory = requiredRemainingMemory;
      this.requiredRemainingTmpSpace = requiredRemainingTmpSpace;
      this.priority = priority;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  // mock Filter class.
  static class MockFilter
      extends CandidateFilter<MockExecutorObject, MockFlowObject> {

    public MockFilter() {
    }

    @Override
    public String getName() {
      return "Mockfilter";
    }

    // function to register the remainingMemory filter.
    // for test purpose the registration is put in a separated method, in production the work should be done
    // in the constructor.
    public void registerFilterforTotalMemory() {
      this.registerFactorFilter(
          FactorFilter.create("requiredTotalMemory", (itemToCheck, sourceObject) -> {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject) {
              return false;
            }

            // Box has infinite memory.:)
            if (itemToCheck.percentOfRemainingMemory == 0) {
              return true;
            }

            // calculate the memory and return.
            return itemToCheck.amountOfRemainingMemory / itemToCheck.percentOfRemainingMemory * 100
                >
                sourceObject.requiredTotalMemory;
          }));
    }

    public void registerFilterforRemainingMemory() {
      this.registerFactorFilter(
          FactorFilter.create("requiredRemainingMemory", (itemToCheck, sourceObject) -> {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject) {
              return false;
            }
            return itemToCheck.amountOfRemainingMemory > sourceObject.requiredRemainingMemory;
          }));
    }

    public void registerFilterforPriority() {
      this.registerFactorFilter(
          FactorFilter.create("requiredProprity", (itemToCheck, sourceObject) -> {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject) {
              return false;
            }

            // priority value, the bigger the lower.
            return itemToCheck.priority >= sourceObject.priority;
          }));
    }

    public void registerFilterforRemainingTmpSpace() {
      this.registerFactorFilter(
          FactorFilter.create("requiredRemainingTmpSpace", (itemToCheck, sourceObject) -> {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject) {
              return false;
            }

            return itemToCheck.remainingTmp > sourceObject.requiredRemainingTmpSpace;
          }));
    }

  }

  // mock comparator class.
  static class MockComparator
      extends CandidateComparator<MockExecutorObject> {

    public MockComparator() {
    }

    @Override
    public String getName() {
      return "MockComparator";
    }

    @Override
    protected boolean tieBreak(final MockExecutorObject object1, final MockExecutorObject object2) {
      if (null == object2) {
        return true;
      }
      if (null == object1) {
        return false;
      }
      return object1.name.compareTo(object2.name) >= 0;
    }

    public void registerComparerForMemory(final int weight) {
      this.registerFactorComparator(FactorComparator.create("Memory", weight, (o1, o2) -> {
        int result = 0;

        // check remaining amount of memory.
        result = o1.amountOfRemainingMemory - o2.amountOfRemainingMemory;
        if (result != 0) {
          return result > 0 ? 1 : -1;
        }

        // check remaining % .
        result = (int) (o1.percentOfRemainingMemory - o2.percentOfRemainingMemory);
        return result == 0 ? 0 : result > 0 ? 1 : -1;

      }));
    }

    public void registerComparerForRemainingSpace(final int weight) {
      this.registerFactorComparator(FactorComparator.create("RemainingTmp", weight, (o1, o2) -> {
        int result = 0;

        // check remaining % .
        result = (int) (o1.remainingTmp - o2.remainingTmp);
        return result == 0 ? 0 : result > 0 ? 1 : -1;

      }));
    }

    public void registerComparerForPriority(final int weight) {
      this.registerFactorComparator(FactorComparator.create("Priority", weight, (o1, o2) -> {
        int result = 0;

        // check priority, bigger the better.
        result = (int) (o1.priority - o2.priority);
        return result == 0 ? 0 : result > 0 ? 1 : -1;

      }));
    }
  }

}
