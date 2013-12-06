package azkaban.test.execapp.event;

import org.junit.Assert;
import org.junit.Test;

import azkaban.execapp.event.BlockingStatus;
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
	
	@Test
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
	
	@Test
	public void testUnfinishedBlock() throws InterruptedException {
		BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);
		
		WatchingThread thread = new WatchingThread(status);
		thread.start();
	
		synchronized(this) {
			wait(3000);
		}
	
		status.changeStatus(Status.SUCCEEDED);
		thread.join();
		
		System.out.println("Diff " + thread.getDiff());
		Assert.assertTrue(thread.getDiff() >= 3000 && thread.getDiff() < 3100);
	}
	
	@Test
	public void testUnfinishedBlockSeveralChanges() throws InterruptedException {
		BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);
		
		WatchingThread thread = new WatchingThread(status);
		thread.start();
	
		synchronized(this) {
			wait(3000);
		}
	
		status.changeStatus(Status.PAUSED);
		
		synchronized(this) {
			wait(1000);
		}
		
		status.changeStatus(Status.FAILED);
		
		thread.join(1000);
		
		System.out.println("Diff " + thread.getDiff());
		Assert.assertTrue(thread.getDiff() >= 4000 && thread.getDiff() < 4100);
	}
	
	@Test
	public void testMultipleWatchers() throws InterruptedException {
		BlockingStatus status = new BlockingStatus(1, "test", Status.QUEUED);
		
		WatchingThread thread1 = new WatchingThread(status);
		thread1.start();

		synchronized(this) {
			wait(2000);
		}
	
		WatchingThread thread2 = new WatchingThread(status);
		thread2.start();
		
		synchronized(this) {
			wait(2000);
		}
		
		status.changeStatus(Status.FAILED);
		thread2.join(1000);
		thread1.join(1000);
		
		System.out.println("Diff thread 1 " + thread1.getDiff());
		System.out.println("Diff thread 2 " + thread2.getDiff());
		Assert.assertTrue(thread1.getDiff() >= 4000 && thread1.getDiff() < 4100);
		Assert.assertTrue(thread2.getDiff() >= 2000 && thread2.getDiff() < 2100);
	}
}
