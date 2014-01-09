package azkaban.test.utils.cache;

import org.junit.Assert;
import org.junit.Test;

import azkaban.utils.cache.Cache;
import azkaban.utils.cache.Cache.EjectionPolicy;
import azkaban.utils.cache.CacheManager;

public class CacheTest {
	@Test
	public void testLRU() {
		CacheManager manager = CacheManager.getInstance();
		Cache cache = manager.createCache();
		cache.setEjectionPolicy(EjectionPolicy.LRU);
		cache.setMaxCacheSize(4);

		cache.insertElement("key1", "val1");
		cache.insertElement("key2", "val2");
		cache.insertElement("key3", "val3");
		cache.insertElement("key4", "val4");

		Assert.assertEquals(cache.get("key2"), "val2");
		Assert.assertEquals(cache.get("key3"), "val3");
		Assert.assertEquals(cache.get("key4"), "val4");
		Assert.assertEquals(cache.get("key1"), "val1");
		Assert.assertEquals(4, cache.getSize());

		cache.insertElement("key5", "val5");
		Assert.assertEquals(4, cache.getSize());
		Assert.assertEquals(cache.get("key3"), "val3");
		Assert.assertEquals(cache.get("key4"), "val4");
		Assert.assertEquals(cache.get("key1"), "val1");
		Assert.assertEquals(cache.get("key5"), "val5");
		Assert.assertNull(cache.get("key2"));
	}

	@Test
	public void testFIFO() {
		CacheManager manager = CacheManager.getInstance();
		Cache cache = manager.createCache();
		cache.setEjectionPolicy(EjectionPolicy.FIFO);
		cache.setMaxCacheSize(4);

		cache.insertElement("key1", "val1");
		synchronized (this) {
			try {
				wait(10);
			} catch (InterruptedException e) {
			}
		}
		cache.insertElement("key2", "val2");
		cache.insertElement("key3", "val3");
		cache.insertElement("key4", "val4");

		Assert.assertEquals(cache.get("key2"), "val2");
		Assert.assertEquals(cache.get("key3"), "val3");
		Assert.assertEquals(cache.get("key4"), "val4");
		Assert.assertEquals(cache.get("key1"), "val1");
		Assert.assertEquals(4, cache.getSize());

		cache.insertElement("key5", "val5");
		Assert.assertEquals(4, cache.getSize());
		Assert.assertEquals(cache.get("key3"), "val3");
		Assert.assertEquals(cache.get("key4"), "val4");
		Assert.assertEquals(cache.get("key2"), "val2");
		Assert.assertEquals(cache.get("key5"), "val5");
		Assert.assertNull(cache.get("key1"));
	}

	@Test
	public void testTimeToLiveExpiry() {
		CacheManager.setUpdateFrequency(200);
		CacheManager manager = CacheManager.getInstance();
		Cache cache = manager.createCache();
		
		cache.setUpdateFrequencyMs(200);
		cache.setEjectionPolicy(EjectionPolicy.FIFO);
		cache.setExpiryTimeToLiveMs(4500);
		cache.insertElement("key1", "val1");

		synchronized (this) {
			try {
				wait(1000);
			} catch (InterruptedException e) {
			}
		}
		Assert.assertEquals(cache.get("key1"), "val1");
		cache.insertElement("key2", "val2");
		synchronized (this) {
			try {
				wait(4000);
			} catch (InterruptedException e) {
			}
		}
		Assert.assertNull(cache.get("key1"));
		Assert.assertEquals("val2", cache.get("key2"));

		synchronized (this) {
			try {
				wait(1000);
			} catch (InterruptedException e) {
			}
		}

		Assert.assertNull(cache.get("key2"));
	}

	@Test
	public void testIdleExpireExpiry() {
		CacheManager.setUpdateFrequency(250);
		CacheManager manager = CacheManager.getInstance();
		Cache cache = manager.createCache();
		
		cache.setUpdateFrequencyMs(250);
		cache.setEjectionPolicy(EjectionPolicy.FIFO);
		cache.setExpiryIdleTimeMs(4500);
		cache.insertElement("key1", "val1");
		cache.insertElement("key3", "val3");
		synchronized (this) {
			try {
				wait(1000);
			} catch (InterruptedException e) {
			}
		}
		Assert.assertEquals(cache.get("key1"), "val1");
		cache.insertElement("key2", "val2");
		synchronized (this) {
			try {
				wait(4000);
			} catch (InterruptedException e) {
			}
		}
		Assert.assertEquals("val1", cache.get("key1"));
		Assert.assertNull(cache.get("key3"));
		synchronized (this) {
			try {
				wait(1000);
			} catch (InterruptedException e) {
			}
		}

		Assert.assertNull(cache.get("key2"));
	}
}
