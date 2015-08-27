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
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.executor.dispatcher.*;

public class DispatcherTest {
  // mock executor object.
  protected class MockExecutorObject{
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
          public boolean analyzeTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
            // REAL LOGIC COMES HERE -
            if (null == itemToCheck || null == sourceObject){
              return false;
            }

            // Box has infinite memory.:)
            if (itemToCheck.percentOfRemainingMemory == 0) {
              return true;
            }

            // calculate the memory and return.
            return itemToCheck.amountOfRemainingMemory / itemToCheck.percentOfRemainingMemory * 100 > sourceObject.requiredTotalMemory;
          }}));
    }

    public void registerFilterforRemainingMemory(){
      this.registerFactorFilter(FactorFilter.create("requiredRemainingMemory",
          new FactorFilter.Filter<MockExecutorObject,MockFlowObject>() {
        public boolean analyzeTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
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
        public boolean analyzeTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
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
        public boolean analyzeTarget(MockExecutorObject itemToCheck, MockFlowObject sourceObject) {
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

  @Before
  public void setUp() throws Exception {
    BasicConfigurator.configure();

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
      boolean result = mFilter.analyzeTarget(this.getExecutorByName("Executor1"), dispatchingObj);
      Assert.assertTrue(result);

      //expect true.
      result = mFilter.analyzeTarget(this.getExecutorByName("Executor3"), dispatchingObj);
      /*
      1 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - start checking 'Executor3' with factor filter for 'Mockfilter'
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - Final checking result : true
      */
      Assert.assertTrue(result);

      // add the priority filter.
      mFilter.registerFilterforPriority();
      result = mFilter.analyzeTarget(this.getExecutorByName("Executor3"), dispatchingObj);
      // expect false, for priority.
      /*
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - start checking 'Executor3' with factor filter for 'Mockfilter'
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredProprity] filter result : false
      2 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - Final checking result : false
      */
      Assert.assertFalse(result);

      // add the remaining space filter.
      mFilter.registerFilterforRemainingTmpSpace();

      // expect pass.
      result = mFilter.analyzeTarget(this.getExecutorByName("Executor2"), dispatchingObj);
      /*
      3 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - start checking 'Executor2' with factor filter for 'Mockfilter'
      3 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      3 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingTmpSpace] filter result : true
      3 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredProprity] filter result : true
      3 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - Final checking result : true
      */
      Assert.assertTrue(result);

      // expect false, remaining tmp, priority will also fail but the logic shortcuts when the Tmp size check Fails.
      result = mFilter.analyzeTarget(this.getExecutorByName("Executor8"), dispatchingObj);
      /*
      4 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - start checking 'Executor8' with factor filter for 'Mockfilter'
      4 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingMemory] filter result : true
      4 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - [Factor: requiredRemainingTmpSpace] filter result : false
      4 [main] INFO azkaban.executor.dispatcher.CandidateFilter  - Final checking result : false
      */
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
  public void testDispatcher() throws Exception {
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(3);
    comparator.registerComparerForPriority(5);
    comparator.registerComparerForRemainingSpace(3);

    ExecutorDispatcher<MockExecutorObject,MockFlowObject> morkDispatcher =
        new ExecutorDispatcher<MockExecutorObject,MockFlowObject>(filter,comparator);

    // mock object, remaining memory 11500, total memory 3095, remainingTmpSpace 4200, priority 2.
    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",3096, 1500,4200,2);

    // expected selection = #12
    MockExecutorObject nextExecutor = morkDispatcher.getNext(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor1"),nextExecutor);

   // remaining memory 11500, total memory 3095, remainingTmpSpace 14200, priority 2.
   dispatchingObj = new MockFlowObject("flow1",3096, 1500,14200,2);
   // all candidates should be filtered by the remaining memory.
   nextExecutor = morkDispatcher.getNext(this.executorList, dispatchingObj);
   Assert.assertEquals(null,nextExecutor);
  }

  @Test
  public void testDispatcherChangingFactorWeight() throws Exception {
    MockFilter filter = new MockFilter();
    MockComparator comparator = new MockComparator();

    filter.registerFilterforPriority();
    filter.registerFilterforRemainingMemory();
    filter.registerFilterforRemainingTmpSpace();
    filter.registerFilterforTotalMemory();

    comparator.registerComparerForMemory(1);
    comparator.registerComparerForPriority(1);
    comparator.registerComparerForRemainingSpace(1);

    ExecutorDispatcher<MockExecutorObject,MockFlowObject> morkDispatcher =
        new ExecutorDispatcher<MockExecutorObject,MockFlowObject>(filter,comparator);

    MockFlowObject  dispatchingObj = new MockFlowObject("flow1",100, 100,100,3);
    MockExecutorObject executor = morkDispatcher.getNext(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor9"), executor);

    // adjusted the weight for memory to 10, therefore item #11 should be returned.
    morkDispatcher.getComparator().adjustFactorWeight("Memory", 10);
    executor = morkDispatcher.getNext(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor11"), executor);

    // adjusted the weight for memory back to 1, therefore item #12 should be returned.
    morkDispatcher.getComparator().adjustFactorWeight("Memory", 1);
    executor = morkDispatcher.getNext(this.executorList, dispatchingObj);
    Assert.assertEquals(this.getExecutorByName("Executor9"), executor);

  }
}
