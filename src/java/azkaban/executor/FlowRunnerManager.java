/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.log4j.Logger;

import azkaban.utils.Utils;
import azkaban.executor.ExecutableFlow.Status;
import azkaban.executor.event.Event;
import azkaban.executor.event.Event.Type;
import azkaban.executor.event.EventListener;
import azkaban.utils.ExecutableFlowLoader;
import azkaban.utils.Mailman;
import azkaban.utils.Props;

/**
 * Execution manager for the server side execution.
 * 
 */
public class FlowRunnerManager {
	private static Logger logger = Logger.getLogger(FlowRunnerManager.class);
	private File basePath;

	private static final int DEFAULT_NUM_EXECUTING_FLOWS = 30;
	private ConcurrentHashMap<String, FlowRunner> runningFlows = new ConcurrentHashMap<String, FlowRunner>();
	private LinkedBlockingQueue<FlowRunner> queue = new LinkedBlockingQueue<FlowRunner>();
	private int numThreads = DEFAULT_NUM_EXECUTING_FLOWS;

	private ExecutorService executorService;
	private SubmitterThread submitterThread;
	private FlowRunnerEventListener eventListener;

	private Mailman mailer;
	private String senderAddress;
	private String clientHostname;
	private String clientPortNumber;

	private Props globalProps;

	// Keep recent flows only one minute after it finished.
	private CacheManager manager = CacheManager.create();
	private Cache recentFlowsCache;
	private static final int LIVE_SECONDS = 60;
	private static final int RECENT_FLOWS_CACHE_SIZE = 100;
	
	private boolean testMode = false;
	
	public FlowRunnerManager(Props props, Props globalProps, Mailman mailer) {
		this.mailer = mailer;

		this.senderAddress = props.getString("mail.sender");
		this.clientHostname = props.getString("jetty.hostname", "localhost");
		this.clientPortNumber = Utils.nonNull(props.getString("jetty.ssl.port"));

		setupCache();
		
		testMode = props.getBoolean("test.mode", false);
		if (testMode) {
			logger.info("Running in testMode.");
		}

		this.globalProps = globalProps;
		
		basePath = new File(props.getString("execution.directory"));
		numThreads = props.getInt("executor.flow.threads", DEFAULT_NUM_EXECUTING_FLOWS);
		executorService = Executors.newFixedThreadPool(numThreads);
		eventListener = new FlowRunnerEventListener(this);

		submitterThread = new SubmitterThread(queue);
		submitterThread.start();
	}

	private void setupCache() {
		CacheConfiguration cacheConfig = new CacheConfiguration("recentFlowsCache",RECENT_FLOWS_CACHE_SIZE)
				.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO)
				.overflowToDisk(false)
				.eternal(false)
				.timeToLiveSeconds(LIVE_SECONDS)
				.diskPersistent(false)
				.diskExpiryThreadIntervalSeconds(0);

		recentFlowsCache = new Cache(cacheConfig);
		manager.addCache(recentFlowsCache);
	}
	
	public void submitFlow(String id, String path) throws ExecutorManagerException {
		// Load file and submit
		logger.info("Flow " + id + " submitted with path " + path);

		File dir = new File(path);
		ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(dir);
		flow.setExecutionPath(path);

		FlowRunner runner = new FlowRunner(flow);
		runner.setTestMode(testMode);

		runningFlows.put(id, runner);
		runner.setGlobalProps(globalProps);
		runner.addListener(eventListener);
		executorService.submit(runner);
	}

	public void cancelFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.cancel(user);
		}
	}

	public void pauseFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.pause(user);
		}
	}

	public void resumeFlow(String id, String user) throws ExecutorManagerException {
		FlowRunner runner = runningFlows.get(id);
		if (runner != null) {
			runner.resume(user);
		}
	}

	public FlowRunner getFlowRunner(String id) {
		return runningFlows.get(id);
	}

	public ExecutableFlow getExecutableFlow(String id) {
		FlowRunner runner = runningFlows.get(id);
		if (runner == null) {
			Element elem = recentFlowsCache.get(id);
			if (elem == null) {
				return null;
			}
			return (ExecutableFlow)elem.getObjectValue();
		}

		return runner.getFlow();
	}

	private class SubmitterThread extends Thread {
		private BlockingQueue<FlowRunner> queue;
		private boolean shutdown = false;

		public SubmitterThread(BlockingQueue<FlowRunner> queue) {
			this.queue = queue;
		}

		public void shutdown() {
			shutdown = true;
			this.interrupt();
		}

		public void run() {
			while (!shutdown) {
				try {
					FlowRunner flowRunner = queue.take();
					executorService.submit(flowRunner);
				} catch (InterruptedException e) {
					logger.info("Interrupted. Probably to shut down.");
				}
			}
		}
	}

	private class FlowRunnerEventListener implements EventListener {
		private FlowRunnerManager manager;

		public FlowRunnerEventListener(FlowRunnerManager manager) {
			this.manager = manager;
		}

		@Override
		public synchronized void handleEvent(Event event) {
			FlowRunner runner = (FlowRunner) event.getRunner();
			ExecutableFlow flow = runner.getFlow();

			System.out.println("Event " + flow.getExecutionId() + " " + flow.getFlowId() + " " + event.getType());
			if (event.getType() == Type.FLOW_FINISHED) {
				if (flow.getStatus() == Status.SUCCEEDED)
					sendSuccessEmail(runner);
				else
					sendErrorEmail(runner);

				logger.info("Flow " + flow.getExecutionId() + " has finished.");
				runningFlows.remove(flow.getExecutionId());
				recentFlowsCache.put(new Element(flow.getExecutionId(), flow));
			}
		}
	}

	private List<String> getLogURLs(FlowRunner runner) {
		List<String> logURLs = new ArrayList<String>();

		String flowID = runner.getFlow().getFlowId();
		String execID = runner.getFlow().getExecutionId();
		List<String> jobIDs = runner.getJobsFinished();

		// first construct log URL;
		String logURL = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execID
				+ "#log";
		logURLs.add(logURL);
		// then the individual jobs log URL that actually ran
		for (String jobID : jobIDs) {
			String jobLog = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid="
					+ execID + "&flow=" + flowID + "&job=" + jobID;
			logURLs.add(jobLog);
		}

		return logURLs;
	}

	/*
	 * Wrap a single exception with the name of the scheduled job
	 */
	private void sendErrorEmail(FlowRunner runner) {
		ExecutableFlow flow = runner.getFlow();
		List<String> emailList = flow.getFailureEmails();
		if (emailList != null && !emailList.isEmpty() && mailer != null) {

			try {

				String subject = "Flow '" + flow.getFlowId() + "' has completed on "
						+ InetAddress.getLocalHost().getHostName() + "!";
				String body = "The Flow '" + flow.getFlowId() + "' failed. \n See logs below: \n";
				for (String URL : getLogURLs(runner)) {
					body += (URL + "\n");
				}

				if (!testMode) {
					mailer.sendEmailIfPossible(senderAddress, emailList, subject, body);
				}
			} catch (UnknownHostException uhe) {
				logger.error(uhe);
			} catch (Exception e) {
				logger.error(e);
			}
		}
	}

	private void sendSuccessEmail(FlowRunner runner) {

		ExecutableFlow flow = runner.getFlow();

		List<String> emailList = flow.getSuccessEmails();

		if (emailList != null && !emailList.isEmpty() && mailer != null) {
			try {

				String subject = "Flow '" + flow.getFlowId() + "' has completed on "
						+ InetAddress.getLocalHost().getHostName() + "!";
				String body = "The Flow '" + flow.getFlowId() + "' succeeded. \n See logs below: \n";
				for (String URL : getLogURLs(runner)) {
					body += (URL + "\n");
				}

				if (!testMode) {
					mailer.sendEmailIfPossible(senderAddress, emailList, subject, body);
				}
			} catch (UnknownHostException uhe) {
				logger.error(uhe);
			} catch (Exception e) {
				logger.error(e);
			}
		}
	}

}
