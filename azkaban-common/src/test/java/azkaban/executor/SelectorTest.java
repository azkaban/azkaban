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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

import azkaban.executor.selector.*;
import azkaban.utils.JSONUtils;

public class SelectorTest {
  // mock executor object.
  protected class MockExecutorObject implements Comparable <MockExecutorObject>{
    public String name;
    public int    port;
    public double percentOfRemainingMemory;
    public int    amountOfRemainingMemory;
    public int    priority;
    public Date   lastAssigned;
    public double percentOfRemainingFlowcapacity;
    public int    remainingTmp;

    public MockExecutorObject(String name,
        int port,
        double percentOfRemainingMemory,
        int amountOfRemainingMemory,
        int priority,
        Date lastAssigned,
        double percentOfRemainingFlowcapacity,
        int remainingTmp)
    {
      this.name = name;
      this.port = port;
      this.percentOfRemainingMemory = percentOfRemainingMemory;
      this.amountOfRemainingMemory =amountOfRemainingMemory;
      this.priority = priority;
      this.lastAssigned = lastAssigned;
      this.percentOfRemainingFlowcapacity = percentOfRemainingFlowcapacity;
      this.remainingTmp = remainingTmp;
    }

    @Override
    public String toString()
    {
      return this.name;
    }

    @Override
    public int compareTo(MockExecutorObject o) {
      return null == o ? 1 : this.hashCode() - o.hashCode();
    }
  }

  // Mock flow object.
  protected class MockFlowObject{
    public String name;
    public int    requiredRemainingMemory;
    public int    requiredTotalMemory;
    public int    requiredRemainingTmpSpace;
    public int    priority;

    public MockFlowObject(String name,
        int requiredTotalMemory,
        int requiredRemainingMemory,
        int requiredRemainingTmpSpace,
        int priority)
    {
      this.name = name;
      this.requiredTotalMemory = requiredTotalMemory;
      this.requiredRemainingMemory = requiredRemainingMemory;
      this.requiredRemainingTmpSpace = requiredRemainingTmpSpace;
      this.priority = priority;
    }

    @Override
    public String toString()
    {
      return this.name;
    }
  }

  // mock Filter class.
  protected class MockFilter
  extends CandidateFilter<MockExecutorObject,MockFlowObject>{

    @Override
    public String getName() {
      return "Mockfilter";
    }

    public MockFilter(){
    }

    // function to register the remainingMemory filter.
    // for test purpose the registration is put in a separated method, in production the work should be done
    // in the constructor.
    public void registerFilterforTotalMemory(){
      this.registerFactorFilter(FactorFilter.create("requiredTotalMemory",
          new FactorFilter.Filter<MockExecutorObject,MockFlowObject>() {
          public boolean filterTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject){
              return false;
            }

            // Box has infinite memory.:)
            if (itemToCheck.percentOfRemainingMemory == 0) {
              return true;
            }

            // calculate the memory and return.
            return itemToCheck.amountOfRemainingMemory / itemToCheck.percentOfRemainingMemory * 100 >
                   sourceObject.requiredTotalMemory;
          }}));
    }

    public void registerFilterforRemainingMemory(){
      this.registerFactorFilter(FactorFilter.create("requiredRemainingMemory",
          new FactorFilter.Filter<MockExecutorObject,MockFlowObject>() {
        public boolean filterTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
          // REAL LOGIC COMES HERE -
          if (null == itemToCheck || null == sourceObject){
            return false;
         }
         return itemToCheck.amountOfRemainingMemory > sourceObject.requiredRemainingMemory;
        }}));
    }

    public void registerFilterforPriority(){
      this.registerFactorFilter(FactorFilter.create("requiredProprity",
          new FactorFilter.Filter<MockExecutorObject,MockFlowObject>() {
        public boolean filterTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
          // REAL LOGIC COMES HERE -
          if (null == itemToCheck || null == sourceObject){
            return false;
          }

          // priority value, the bigger the lower.
          return itemToCheck.priority >= sourceObject.priority;
        }}));
    }

    public void registerFilterforRemainingTmpSpace(){
      this.registerFactorFilter(FactorFilter.create("requiredRemainingTmpSpace",
          new FactorFilter.Filter<MockExecutorObject,MockFlowObject>() {
        public boolean filterTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
          // REAL LOGIC COMES HERE -
          if (null == itemToCheck || null == sourceObject){
            return false;
          }

         return itemToCheck.remainingTmp > sourceObject.requiredRemainingTmpSpace;
        }}));
    }

  }

  // mock comparator class.
  protected class MockComparator
  extends CandidateComparator<MockExecutorObject>{

    @Override
    public String getName() {
      return "MockComparator";
    }

    @Override
    protected boolean tieBreak(MockExecutorObject object1, MockExecutorObject object2){
      if (null == object2) return true;
      if (null == object1) return false;
      return object1.name.compareTo(object2.name) >= 0;
    }

    public MockComparator(){
    }

    public void registerComparerForMemory(int weight){
      this.registerFactorComparator(FactorComparator.create("Memory", weight, new Comparator<MockExecutorObject>(){
        public int compare(MockExecutorObject o1, MockExecutorObject o2) {
          int result = 0 ;

          // check remaining amount of memory.
          result = o1.amountOfRemainingMemory - o2.amountOfRemainingMemory;
          if (result != 0){
            return result > 0 ? 1 : -1;
          }

          // check remaining % .
          result = (int)(o1.percentOfRemainingMemory - o2.percentOfRemainingMemory);
          return result == 0 ? 0 : result > 0 ? 1 : -1;

        } }));
    }

    public void registerComparerForRemainingSpace(int weight){
      this.registerFactorComparator(FactorComparator.create("RemainingTmp", weight, new Comparator<MockExecutorObject>(){
        public int compare(MockExecutorObject o1, MockExecutorObject o2) {
          int result = 0 ;

          // check remaining % .
          result = (int)(o1.remainingTmp - o2.remainingTmp);
          return result == 0 ? 0 : result > 0 ? 1 : -1;

        } }));
    }

    public void registerComparerForPriority(int weight){
      this.registerFactorComparator(FactorComparator.create("Priority", weight, new Comparator<MockExecutorObject>(){
        public int compare(MockExecutorObject o1, MockExecutorObject o2) {
          int result = 0 ;

          // check priority, bigger the better.
          result = (int)(o1.priority - o2.priority);
          return result == 0 ? 0 : result > 0 ? 1 : -1;

        } }));
    }
  }

  // test samples.
  protected ArrayList<MockExecutorObject> executorList = new ArrayList<MockExecutorObject>();

  @BeforeClass public static void onlyOnce() {
    BasicConfigurator.configure();
   }

  @Before
  public void setUp() throws Exception {
    executorList.clear();
    executorList.add(new MockExecutorObject("Executor1",8080,50.0,2048,5,new Date(), 20, 6400));
    executorList.add(new MockExecutorObject("Executor2",8080,50.0,2048,4,new Date(), 20, 6400));
    executorList.add(new MockExecutorObject("Executor3",8080,40.0,2048,1,new Date(), 20, 6400));
    executorList.add(new MockExecutorObject("Executor4",8080,50.0,2048,4,new Date(), 20, 6400));
    executorList.add(new MockExecutorObject("Executor5",8080,50.0,1024,5,new Date(), 90, 6400));
    executorList.add(new MockExecutorObject("Executor6",8080,50.0,1024,5,new Date(), 90, 3200));
    executorList.add(new MockExecutorObject("Executor7",8080,50.0,1024,5,new Date(), 90, 3200));
    executorList.add(new MockExecutorObject("Executor8",8080,50.0,2048,1,new Date(), 90, 3200));
    executorList.add(new MockExecutorObject("Executor9",8080,50.0,2050,5,new Date(), 90, 4200));
    executorList.add(new MockExecutorObject("Executor10",8080,00.0,1024,1,new Date(), 90, 3200));
    executorList.add(new MockExecutorObject("Executor11",8080,20.0,2096,3,new Date(), 90, 2400));
    executorList.add(new MockExecutorObject("Executor12",8080,90.0,2050,5,new Date(), 60, 2500));


    // make sure each time the order is different.
    Collections.shuffle(this.executorList);
  }

  private MockExecutorObject  getExecutorByName(String name){
    MockExecutorObject returnVal = null;
    for (MockExecutorObject item : this.executorList){
      if (item.name.equals(name)){
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
      MockFlowObject  dispatchingObj = new MockFlowObject("flow1",3096, 1500,4200,2);

      MockFilter mFilter = new MockFilter();
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
    MockFilter filter = new MockFilter();
    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();
    boolean result = false;
    try {
        result = filter.filterTarget(this.getExecutorByName("Executor1"), null);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when null value is passed to the filter.");
      }
    // note : the FactorFilter logic will decide whether true or false should be returned when null value
    //        is passed, for the Mock class it returns false.
    Assert.assertFalse(result);

    try {
        result = filter.filterTarget(null, null);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when null value is passed to the filter.");
      }
    // note : the FactorFilter logic will decide whether true or false should be returned when null value
    //        is passed, for the Mock class it returns false.
    Assert.assertFalse(result);
  }

  @Test
  public void testExecutorComparer() throws Exception {
    MockComparator comparator = new MockComparator();
    comparator.registerComparerForMemory(5);

    MockExecutorObject nextExecutor = Collections.max(this.executorList, comparator);

    // expect the first item to be selected, memory wise it is the max.
    Assert.assertEquals(this.getExecutorByName("Executor11"),nextExecutor);

    // add the priority factor.
    // expect again the #9 item to be selected.
    comparator.registerComparerForPriority(6);
    nextExecutor = Collections.max(this.executorList, comparator);
    Assert.assertEquals(this.getExecutorByName("Executor12"),nextExecutor);

    // add the remaining space factor.
    // expect the #12 item to be returned.
    comparator.registerComparerForRemainingSpace(3);
    nextExecutor = Collections.max(this.executorList, comparator);
    Assert.assertEquals(this.getExecutorByName("Executor12"),nextExecutor);
  }

  @Test
  public void testExecutorComparerResisterComparerWInvalidWeight() throws Exception {
    MockComparator comparator = new MockComparator();
    comparator.registerComparerForMemory(0);
  }

  @Test
  public void testSelector() throws Exception {
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(5);
    comparator.registerComparerForRemainingSpace(3);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(filter,comparator);

    // mock object, remaining memory 11500, total memory 3095, remainingTmpSpace 4200, priority 2.
    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",3096, 1500,4200,2);

    // expected selection = #12
    MockExecutorObject nextExecutor = morkSelector.getBest(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor1"),nextExecutor);

   // remaining memory 11500, total memory 3095, remainingTmpSpace 14200, priority 2.
   dispatchingObj = new MockFlowObject("flow1",3096, 1500,14200,2);
   // all candidates should be filtered by the remaining memory.
   nextExecutor = morkSelector.getBest(this.executorList, dispatchingObj);
   Assert.assertEquals(null,nextExecutor);
  }

  @Test
  public void testSelectorsignleCandidate() throws Exception {
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(filter,comparator);

    ArrayList<MockExecutorObject> signleExecutorList = new ArrayList<MockExecutorObject>();
    MockExecutorObject signleExecutor = new MockExecutorObject("ExecutorX",8080,50.0,2048,3,new Date(), 20, 6400);
    signleExecutorList.add(signleExecutor);

    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,5);
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
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(filter,comparator);

    ArrayList<MockExecutorObject> list = new ArrayList<MockExecutorObject>();
    MockExecutorObject signleExecutor = new MockExecutorObject("ExecutorX",8080,50.0,2048,3,new Date(), 20, 6400);
    list.add(signleExecutor);
    list.add(signleExecutor);
    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,3);
    MockExecutorObject executor = morkSelector.getBest(list, dispatchingObj);
    Assert.assertTrue(signleExecutor == executor);
  }

  @Test
  public void testSelectorListWithItemsThatAreEqualInValue() throws Exception {
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(filter,comparator);

    // note - as the tieBreaker set in the MockComparator uses the name value of the executor to do the
    //        final diff therefore we need to set the name differently to make a meaningful test, in real
    //        scenario we may want to use something else (say hash code) to be the bottom line for the tieBreaker
    //        to make a final decision, the purpose of the test here is to prove that for two candidates with
    //        exact value (in the case of test, all values except for the name) the decision result is stable.
    ArrayList<MockExecutorObject> list = new ArrayList<MockExecutorObject>();
    MockExecutorObject executor1 = new MockExecutorObject("ExecutorX", 8080,50.0,2048,3,new Date(), 20, 6400);
    MockExecutorObject executor2 = new MockExecutorObject("ExecutorX2",8080,50.0,2048,3,new Date(), 20, 6400);
    list.add(executor1);
    list.add(executor2);
    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,3);
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
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(filter,comparator);

    ArrayList<MockExecutorObject> list = new ArrayList<MockExecutorObject>();

    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,5);

    MockExecutorObject executor  = null;

    try {
      executor = morkSelector.getBest(list, dispatchingObj);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when an empty list is passed to the Selector.");
      }

    // expected to see null result.
    Assert.assertTrue(null == executor);

    try {
      executor = morkSelector.getBest(list, dispatchingObj);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when null is passed to the Selector as the candidate list.");
      }

      // expected to see null result, as the only executor is filtered out .
      Assert.assertTrue(null == executor);

  }

  @Test
  public void testSelectorListWithNullValue() throws Exception {
    MockComparator comparator = new MockComparator();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(4);
    comparator.registerComparerForRemainingSpace(1);

    CandidateSelector<MockExecutorObject,MockFlowObject> morkSelector =
        new CandidateSelector<MockExecutorObject,MockFlowObject>(null,comparator);

    ArrayList<MockExecutorObject> list = new ArrayList<MockExecutorObject>();
    MockExecutorObject executor1 = new MockExecutorObject("ExecutorX", 8080,50.0,2048,3,new Date(), 20, 6400);
    MockExecutorObject executor2 = new MockExecutorObject("ExecutorX2",8080,50.0,2048,3,new Date(), 20, 6400);
    list.add(executor1);
    list.add(executor2);
    list.add(null);

    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,3);
    MockExecutorObject executor  = null;
    try {
      executor = morkSelector.getBest(list, dispatchingObj);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when an List contains null value.");
      }
    Assert.assertTrue(executor2 == executor);

    // try to compare null vs null, no exception is expected.
    list.clear();
    list.add(null);
    list.add(null);
    try {
      executor = morkSelector.getBest(list, dispatchingObj);
      } catch (Exception ex){
        Assert.fail("no exception should be thrown when an List contains multiple null values.");
      }
    Assert.assertTrue(null == executor);

  }

  @Test
  public void testCreatingExectorfilterObject() throws Exception{
    List<String> validList = new ArrayList<String>(ExecutorFilter.getAvailableFilterNames());
    try {
      new ExecutorFilter(validList);
    }catch (Exception ex){
      Assert.fail("creating ExecutorFilter with valid list throws exception . ex -" + ex.getMessage());
    }
  }

  @Test
  public void testCreatingExectorfilterObjectWInvalidList() throws Exception{
    List<String> invalidList = new ArrayList<String>();
    invalidList.add("notExistingFilter");
    Exception result = null;
    try {
      new ExecutorFilter(invalidList);
    }catch (Exception ex){
      if (ex instanceof IllegalArgumentException)
      result = ex;
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExectorComparatorObject() throws Exception{
   Map<String,Integer> comparatorMap = new HashMap<String,Integer>();
   for (String name : ExecutorComparator.getAvailableComparatorNames()){
     comparatorMap.put(name, 1);
   }
   try {
      new ExecutorComparator(comparatorMap);
    }catch (Exception ex){
      Assert.fail("creating ExecutorComparator with valid list throws exception . ex -" + ex.getMessage());
    }
  }

  @Test
  public void testCreatingExectorComparatorObjectWInvalidName() throws Exception{
    Map<String,Integer> comparatorMap = new HashMap<String,Integer>();
    comparatorMap.put("invalidName", 0);
    Exception result = null;
    try {
      new ExecutorComparator(comparatorMap);
    }catch (Exception ex){
      if (ex instanceof IllegalArgumentException)
      result = ex;
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExectorComparatorObjectWInvalidWeight() throws Exception{
    Map<String,Integer> comparatorMap = new HashMap<String,Integer>();
    for (String name : ExecutorComparator.getAvailableComparatorNames()){
      comparatorMap.put(name, -1);
    }
    Exception result = null;
    try {
      new ExecutorComparator(comparatorMap);
    }catch (Exception ex){
      if (ex instanceof IllegalArgumentException)
      result = ex;
    }
    Assert.assertNotNull(result);
  }

  @Test
  public void testCreatingExecutorSelectorWithEmptyFilterComparatorList() throws Exception{
    List<Executor> executorList = new ArrayList<Executor>();
    executorList.add(new Executor(1, "host1", 80, true));
    executorList.add(new Executor(2, "host2", 80, true));
    executorList.add(new Executor(3, "host3", 80, true));

    executorList.get(0).setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89, 0));
    executorList.get(1).setExecutorInfo(new ExecutorInfo(50, 14095, 50, System.currentTimeMillis(), 90,  0));
    executorList.get(2).setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 90,  0));

    ExecutableFlow flow = new ExecutableFlow();

    ExecutorSelector selector = new ExecutorSelector(null , null);
    Executor executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(2), executor);
  }


  @Test
  public void testExecutorSelectorE2E() throws Exception{
    List<String> filterList = new ArrayList<String>(ExecutorFilter.getAvailableFilterNames());
    Map<String,Integer> comparatorMap = new HashMap<String,Integer>();
    List<Executor> executorList = new ArrayList<Executor>();
    executorList.add(new Executor(1, "host1", 80, true));
    executorList.add(new Executor(2, "host2", 80, true));
    executorList.add(new Executor(3, "host3", 80, true));

    executorList.get(0).setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89, 0));
    executorList.get(1).setExecutorInfo(new ExecutorInfo(50, 14095, 50, System.currentTimeMillis(), 90,  0));
    executorList.get(2).setExecutorInfo(new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 90,  0));

    ExecutableFlow flow = new ExecutableFlow();

    for (String name : ExecutorComparator.getAvailableComparatorNames()){
      comparatorMap.put(name, 1);
    }
    ExecutorSelector selector = new ExecutorSelector(filterList,comparatorMap);
    Executor executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(0), executor);

    // simulate that once the flow is assigned, executor1's remaining TMP storage dropped to 2048
    // now we do the getBest again executor3 is expected to be selected as it has a earlier last dispatched time.
    executorList.get(0).setExecutorInfo(new ExecutorInfo(99.9, 4095, 50, System.currentTimeMillis(), 90, 1));
    executor = selector.getBest(executorList, flow);
    Assert.assertEquals(executorList.get(2), executor);
  }

  @Test
  public void  testExecutorInfoJsonParser() throws Exception{
    ExecutorInfo exeInfo = new ExecutorInfo(99.9, 14095, 50, System.currentTimeMillis(), 89, 10);
    String json = JSONUtils.toJSON(exeInfo);
    ExecutorInfo exeInfo2 = ExecutorInfo.fromJSONString(json);
    Assert.assertTrue(exeInfo.equals(exeInfo2));
  }

}
