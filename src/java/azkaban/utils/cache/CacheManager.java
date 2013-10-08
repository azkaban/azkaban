/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.utils.cache;

import java.util.HashSet;
import java.util.Set;

public class CacheManager {
	// Thread that expires caches at
	private static final long UPDATE_FREQUENCY = 30000; // Every 30 sec by default.

	private long updateFrequency = UPDATE_FREQUENCY;
	private Set<Cache> caches;
	private static CacheManager manager = null;
	private final CacheManagerThread updaterThread;

	private boolean activeExpiry = false;

	public static CacheManager getInstance() {
		if (manager == null) {
			manager = new CacheManager();
		}
		
		return manager;
	}
	
	private CacheManager() {
		updaterThread = new CacheManagerThread();
		caches = new HashSet<Cache>();

		updaterThread.start();
	}

	public static void setUpdateFrequency(long updateFreqMs) {
		manager.internalUpdateFrequency(updateFreqMs);
	}

	public static void shutdown() {
		manager.internalShutdown();
	}

	public Cache createCache() {
		Cache cache = new Cache(manager);
		manager.internalAddCache(cache);
		return cache;
	}

	public void removeCache(Cache cache) {
		manager.internalRemoveCache(cache);
	}

	private void internalUpdateFrequency(long updateFreq) {
		updateFrequency = updateFreq;
		updaterThread.interrupt();
	}

	private void internalAddCache(Cache cache) {
		caches.add(cache);
		updaterThread.interrupt();
	}

	private void internalRemoveCache(Cache cache) {
		caches.remove(cache);
	}

	private synchronized void internalShutdown() {
		updaterThread.shutdown();
	}

	/* package */synchronized void update() {
		boolean activeExpiry = false;
		for (Cache cache : caches) {
			if (cache.getExpireTimeToIdle() > 0
					|| cache.getExpireTimeToLive() > 0) {
				activeExpiry = true;
				break;
			}
		}

		if (this.activeExpiry != activeExpiry && activeExpiry) {
			this.activeExpiry = activeExpiry;
			updaterThread.interrupt();
		}
	}

	private class CacheManagerThread extends Thread {
		private boolean shutdown = false;

		public void run() {
			while (!shutdown) {
				if (activeExpiry) {
					for (Cache cache : caches) {
						cache.expireCache();
					}

					synchronized (this) {
						try {
							wait(updateFrequency);
						} catch (InterruptedException e) {
						}
					}
				} else {
					synchronized (this) {
						try {
							wait();
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}

		public void shutdown() {
			this.shutdown = true;
			updaterThread.interrupt();
		}
	}
}
