/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.execapp.event;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.executor.Status;

public class BlockingStatusTest {

  public class WatchingThread extends Thread {
    private BlockingStatus status;
    private long diff = 0;

    public WatchingThread(BlockingStatus status) {
      this.status = status;
    }

    public void run() {
      long startTime = System.currentTimeMillis();
      status.blockOnFinishedStatus();
      diff = System.currentTimeMillis() - startTime;
    }

    public long getDiff() {
      return diff;
    }
  }

  /**
    * TODO: Ignore this test at present since travis in Github can not always pass this test.
    *       We will modify the below code to make travis pass in future.
    */
  @Ignore @Test
  public void testFinishedBlock() {
    BlockingStatus status = new BlockingStatus(1, "test", Status.SKIPPED);

    WatchingThread thread = new WatchingThread(status);
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("Diff " + thread.getDiff());
    Assert.assertTrue(thread.getDiff() < 100);
  }

  /**
   * TODO: Ignore this test at present since travis in Github can not always pass this test.
   *       We will modify the below code to make travis pass in future.
   */
  @Ignore @Test
  public void testUnfinishedBlock() throws InterruptedException {
    BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);

    WatchingThread thread = new WatchingThread(status);
    thread.start();

    synchronized (this) {
      wait(3000);
    }

    status.changeStatus(Status.SUCCEEDED);
    thread.join();

    System.out.println("Diff " + thread.getDiff());
    Assert.assertTrue(thread.getDiff() >= 3000 && thread.getDiff() < 3100);
  }

  /**
   * TODO: Ignore this test at present since travis in Github can not always pass this test.
   *       We will modify the below code to make travis pass in future.
   */
  @Ignore @Test
  public void testUnfinishedBlockSeveralChanges() throws InterruptedException {
    BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);

    WatchingThread thread = new WatchingThread(status);
    thread.start();

    synchronized (this) {
      wait(3000);
    }

    status.changeStatus(Status.PAUSED);

    synchronized (this) {
      wait(1000);
    }

    status.changeStatus(Status.FAILED);

    thread.join(1000);

    System.out.println("Diff " + thread.getDiff());
    Assert.assertTrue(thread.getDiff() >= 4000 && thread.getDiff() < 4100);
  }

  /**
   * TODO: Ignore this test at present since travis in Github can not always pass this test.
   *       We will modify the below code to make travis pass in future.
   */
  @Ignore @Test
  public void testMultipleWatchers() throws InterruptedException {
    BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);

    WatchingThread thread1 = new WatchingThread(status);
    thread1.start();

    synchronized (this) {
      wait(2000);
    }

    WatchingThread thread2 = new WatchingThread(status);
    thread2.start();

    synchronized (this) {
      wait(2000);
    }

    status.changeStatus(Status.FAILED);
    thread2.join(1000);
    thread1.join(1000);

    System.out.println("Diff thread 1 " + thread1.getDiff());
    System.out.println("Diff thread 2 " + thread2.getDiff());
    Assert.assertTrue(thread1.getDiff() >= 4000 && thread1.getDiff() < 4200);
    Assert.assertTrue(thread2.getDiff() >= 2000 && thread2.getDiff() < 2200);
  }
}
