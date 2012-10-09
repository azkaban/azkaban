package azkaban.executor;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletRequest;

import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;

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
	//private String defaultFailureEmail;
	//private String defaultSuccessEmail;
	private String senderAddress;
	private String clientHostname;
	private String clientPortNumber;
	
	private Props globalProps;
	
	public FlowRunnerManager(Props props, Props globalProps, Mailman mailer) {
		this.mailer = mailer;
//		this.defaultFailureEmail = props.getString("job.failure.email");
//		this.defaultSuccessEmail = props.getString("job.success.email");
		this.senderAddress = props.getString("mail.sender");
		this.clientHostname = props.getString("jetty.hostname", "localhost");
		this.clientPortNumber = Utils.nonNull(props.getString("jetty.ssl.port"));
		
		basePath = new File(props.getString("execution.directory"));
		numThreads = props.getInt("executor.flow.threads", DEFAULT_NUM_EXECUTING_FLOWS);
		executorService = Executors.newFixedThreadPool(numThreads);
		eventListener = new FlowRunnerEventListener(this);
		
		submitterThread = new SubmitterThread(queue);
		submitterThread.start();
	}
	
	public void submitFlow(String id, String path) throws ExecutorManagerException {
		// Load file and submit
		logger.info("Flow " + id + " submitted with path " + path);
		
		File dir = new File(path);
		ExecutableFlow flow = ExecutableFlowLoader.loadExecutableFlowFromDir(dir);
		flow.setExecutionPath(path);
		flow.setGlobalProps(globalProps);
		
		FlowRunner runner = new FlowRunner(flow);
		runningFlows.put(id, runner);
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
			return null;
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
			while(!shutdown) {
				try {
					FlowRunner flowRunner = queue.take();
					executorService.submit(flowRunner);
				} 
				catch (InterruptedException e) {
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
			FlowRunner runner = (FlowRunner)event.getRunner();
			ExecutableFlow flow = runner.getFlow();
			
			
			
			System.out.println("Event " + flow.getExecutionId() + " " + flow.getFlowId() + " " + event.getType());
			if (event.getType() == Type.FLOW_FINISHED) {
				if(flow.getStatus() == Status.SUCCEEDED)
					sendSuccessEmail(runner);
				else sendErrorEmail(runner);
				
				logger.info("Flow " + flow.getExecutionId() + " has finished.");
				runningFlows.remove(flow.getExecutionId());
				
			}
		}
	}
	
	private List<String> getLogURLs(FlowRunner runner)
	{
		List<String> logURLs = new ArrayList<String>();
		
		String flowID = runner.getFlow().getFlowId();
		String execID = runner.getFlow().getExecutionId();
		List<String> jobIDs = runner.getJobsFinished();
		
		//first construct log URL;
		String logURL = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execID + "#log";
		logURLs.add(logURL);
		//then the individual jobs log URL that actually ran
		for(String jobID : jobIDs) {
			String jobLog = "https://" + clientHostname + ":" + clientPortNumber + "/" + "executor?" + "execid=" + execID + "&flow=" + flowID + "&job=" + jobID;
			logURLs.add(jobLog);
		}
		
		return logURLs;
	}
	
    /*
     * Wrap a single exception with the name of the scheduled job
     */
    private void sendErrorEmail(FlowRunner runner) {
    	ExecutableFlow flow = runner.getFlow();
    	List<String> emailList = new ArrayList<String>(runner.getEmails());
        if(emailList != null && !emailList.isEmpty() && mailer != null) {
        	
        	
        	
        	
            try {
            	
            	String subject = "Flow '" + flow.getFlowId() + "' has completed on " + InetAddress.getLocalHost().getHostName() + "!";
            	String body = "The Flow '" + flow.getFlowId() + "' failed. \n See logs below: \n" ;
            	for(String URL : getLogURLs(runner)) {
            		body += (URL + "\n");
            	}
            	
                mailer.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             subject,
                                             body);
            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
            catch (Exception e) {
                logger.error(e);
            }
        }
    }
    

    private void sendSuccessEmail(FlowRunner runner) {
    	
    	ExecutableFlow flow = runner.getFlow();
    	List<String> emailList = new ArrayList<String>(runner.getEmails());
        
        if(emailList != null && !emailList.isEmpty() && mailer != null) {
            try {
            	
            	String subject = "Flow '" + flow.getFlowId() + "' has completed on " + InetAddress.getLocalHost().getHostName() + "!";
            	String body = "The Flow '" + flow.getFlowId() + "' succeeded. \n See logs below: \n" ;
            	for(String URL : getLogURLs(runner)) {
            		body += (URL + "\n");
            	}
            	
                mailer.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             subject,
                                             body);
            } catch(UnknownHostException uhe) {
                logger.error(uhe);
            }
            catch (Exception e) {
                logger.error(e);
            }
        }
    }
    
}
