package azkaban.scheduler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.JSONUtils;
import azkaban.webapp.AzkabanWebServer;

public class ScheduleStatisticManager {
	private static HashMap<Integer, Object> cacheLock = new HashMap<Integer, Object>();
	private static File cacheDirectory;
	private static final int STAT_NUMBERS = 10;

	public static Map<String, Object> getStatistics(int scheduleId, AzkabanWebServer server) {
		if (cacheDirectory == null) {
			setCacheFolder(new File(server.getServerProps().getString("cache.directory", "cache")));
		}
		Map<String, Object> data = loadCache(scheduleId);
		if (data != null) {
			return data;
		}

		// Calculate data and cache it
		data = calculateStats(scheduleId, server);

		saveCache(scheduleId, data);

		return data;
	}

	private static Map<String, Object> calculateStats(int scheduleId, AzkabanWebServer server) {
		Map<String, Object> data = new HashMap<String, Object>();
		ExecutorManager executorManager = server.getExecutorManager();
		ScheduleManager scheduleManager = server.getScheduleManager();
		Schedule schedule = scheduleManager.getSchedule(scheduleId);

		try {
			List<ExecutableFlow> executables = executorManager.getExecutableFlows(schedule.getProjectId(), schedule.getFlowName(), 0, STAT_NUMBERS, Status.SUCCEEDED);

			long average = 0;
			long min = Integer.MAX_VALUE;
			long max = 0;
			if (executables.isEmpty()) {
				average = 0;
				min = 0;
				max = 0;
			}
			else {
				for (ExecutableFlow flow : executables) {
					long time = flow.getEndTime() - flow.getStartTime();
					average += time;
					if (time < min) {
						min = time;
					}
					if (time > max) {
						max = time;
					}
				}
				average /= executables.size();
			}

			data.put("average", average);
			data.put("min", min);
			data.put("max", max);
		} catch (ExecutorManagerException e) {
			e.printStackTrace();
		}

		return data;
	}

	public static void invalidateCache(int scheduleId, File cacheDir) {
		setCacheFolder(cacheDir);
		// This should be silent and not fail
		try {
			Object lock = getLock(scheduleId);
			synchronized (lock) {
				getCacheFile(scheduleId).delete();
			}
			unLock(scheduleId);
		} catch (Exception e) {
		}
	}

	private static void saveCache(int scheduleId, Map<String, Object> data) {
		Object lock = getLock(scheduleId);
		try {
			synchronized (lock) {
				File cache = getCacheFile(scheduleId);
				cache.createNewFile();
				OutputStream output = new FileOutputStream(cache);
				JSONUtils.toJSON(data, output, false);
				output.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		unLock(scheduleId);
	}

	private static Map<String, Object> loadCache(int scheduleId) {
		Object lock = getLock(scheduleId);
		try {
			synchronized (lock) {
				File cache = getCacheFile(scheduleId);
				if (cache.exists() && cache.isFile()) {
					Object dataObj = JSONUtils.parseJSONFromFile(cache);
					if (dataObj instanceof Map<?, ?>) {
						@SuppressWarnings("unchecked")
						Map<String, Object> data = (Map<String, Object>) dataObj;
						return data;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		unLock(scheduleId);
		return null;
	}

	private static File getCacheFile(int scheduleId) {
		cacheDirectory.mkdirs();
		File file = new File(cacheDirectory, scheduleId + ".cache");
		return file;
	}

	private static Object getLock(int scheduleId) {
		Object lock = null;
		synchronized (cacheLock) {
			lock = cacheLock.get(scheduleId);
			if (lock == null) {
				lock = new Object();
				cacheLock.put(scheduleId, lock);
			}
		}

		return lock;
	}

	private static void unLock(int scheduleId) {
		synchronized (cacheLock) {
			cacheLock.remove(scheduleId);
		}
	}

	private static void setCacheFolder(File cacheDir) {
		if (cacheDirectory == null) {
			cacheDirectory = new File(cacheDir, "schedule-statistics");
		}
	}
}
