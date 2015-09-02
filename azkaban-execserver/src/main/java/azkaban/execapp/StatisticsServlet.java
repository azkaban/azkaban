package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.Statistics;
import azkaban.utils.JSONUtils;

public class StatisticsServlet extends HttpServlet  {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = Logger.getLogger(JMXHttpServlet.class);

  /**
   * Handle all get request to Statistics Servlet {@inheritDoc}
   *
   * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
   *      javax.servlet.http.HttpServletResponse)
   */
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    final Statistics stats = new Statistics();

    List<Thread> workerPool = new ArrayList<Thread>();
    workerPool.add(new Thread(new Runnable(){ public void run() {
      fillRemainingMemoryPercent(stats); }},"RemainingMemoryPercent"));

    workerPool.add(new Thread(new Runnable(){ public void run() {
      fillRemainingFlowCapacityAndLastDispatchedTime(stats); }},"RemainingFlowCapacityAndLastDispatchedTime"));

    workerPool.add(new Thread(new Runnable(){ public void run() {
      fillCpuUsage(stats); }},"CpuUsage"));

    // start all the working threads.
    for (Thread thread : workerPool){thread.start();}

    // wait for all the threads to finish their work.
    // NOTE: the result container itself is not thread safe, we are good as for now no
    //       working thread will modify the same property, nor have any of them
    //       need to compute values based on value(s) of other properties.
    for (Thread thread : workerPool){
      try {
        thread.join();
      } catch (InterruptedException e) {
        logger.error(String.format("failed to collect information for %s as the working thread is interrupted.",
            thread.getName()));
      }}

    JSONUtils.toJSON(stats, resp.getOutputStream(), true);
  }

  /**
   * fill the result set with the percent of the remaining system memory on the server.
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work work on the property "remainingMemory" and "remainingMemoryPercent".
   *
   * NOTE:
   * a double value will be used to present the remaining memory,
   *         a returning value of '55.6' means 55.6%
   */
  private void fillRemainingMemoryPercent(Statistics stats){
    if (new File("/bin/bash").exists() &&  new File("/usr/bin/free").exists()) {
      java.lang.ProcessBuilder processBuilder =
          new java.lang.ProcessBuilder("/bin/bash", "-c", "/usr/bin/free -m | grep Mem:");
      try {
        String line = null;
        Process process = processBuilder.start();
        process.waitFor();
        InputStream inputStream = process.getInputStream();
        try {
          java.io.BufferedReader reader =
              new java.io.BufferedReader(new InputStreamReader(inputStream));
          // we expect the call returns and only returns one line.
          line = reader.readLine();
        }finally {
          inputStream.close();
        }

        logger.info("result from bash call - " + null == line ? "(null)" : line);
        // process the output from bash call.
        if (null != line && line.length() > 0) {
          String[] splitedresult = line.split("\\s+");
          if (splitedresult.length == 7){
            // expected return format -
            // "Mem:" | total | used | free | shared | buffers | cached
            Long totalMemory = Long.parseLong(splitedresult[1]);
            Long  freeMemory = Long.parseLong(splitedresult[3]);
            stats.setRemainingMemory(freeMemory);
            stats.setRemainingMemoryPercent(totalMemory == 0? 0 :
              ((double)freeMemory/(double)totalMemory));
          }
        }
      }
      catch (Exception ex){
        logger.error("failed fetch system memory info as exception is captured when fetching result from bash call.");
      }
    } else {
        logger.error("failed fetch system memory info as 'bash' or 'free' can't be found on the current system.");
    }
  }

  /**
   * fill the result set with the remaining flow capacity .
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work on the property "remainingFlowCapacity".
   */
  private void fillRemainingFlowCapacityAndLastDispatchedTime(Statistics stats){
    FlowRunnerManager runnerMgr =  AzkabanExecutorServer.getApp().getFlowRunnerManager();
    stats.setRemainingFlowCapacity(runnerMgr.getMaxNumRunningFlows() -
                                   runnerMgr.getNumRunningFlows() -
                                   runnerMgr.getNumQueuedFlows());
    stats.setLastDispatchedTime(runnerMgr.getLastFlowSubmittedTime());
  }


  /**
   * fill the result set with the Remaining temp Storage .
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work on the property "cpuUdage".
   */
  private void fillCpuUsage(Statistics stats){
    if (new File("/bin/bash").exists() &&  new File("/usr/bin/top").exists()) {
      java.lang.ProcessBuilder processBuilder =
          new java.lang.ProcessBuilder("/bin/bash", "-c", "/usr/bin/top -bn4 | grep \"Cpu(s)\"");
      try {
        ArrayList<String> output = new ArrayList<String>();
        Process process = processBuilder.start();
        process.waitFor();
        InputStream inputStream = process.getInputStream();
        try {
          java.io.BufferedReader reader =
              new java.io.BufferedReader(new InputStreamReader(inputStream));
          String line = null;
          while ((line = reader.readLine()) != null) {
            output.add(line);
          }
        }finally {
          inputStream.close();
        }

        logger.info("lines of the result from bash call - " + output.size());
        // process the output from bash call.
        if (output.size() > 0) {
          double us = 0 ; // user
          double sy = 0 ; // system
          double wi = 0 ; // waiting.
          int   sampleCount = 0;

          // process all the output, we will do 5 samples for the cpu and calculate the avarage.
          for(String line : output){
            String[] splitedresult = line.split("\\s+");
            if (splitedresult.length == 9){
              // expected return format -
              // Cpu(s):  1.4%us,  0.1%sy,  0.0%ni, 98.5%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
              double tmp_us = 0 ; // user
              double tmp_sy = 0 ; // system
              double tmp_wi = 0 ; // waiting.
              try {
              tmp_us = Double.parseDouble(splitedresult[1].split("%")[0]);
              tmp_sy = Double.parseDouble(splitedresult[2].split("%")[0]);
              tmp_wi = Double.parseDouble(splitedresult[5].split("%")[0]);
              } catch(NumberFormatException e){
                logger.error("skipping the line from the output cause it is in unexpected format -" + line);
                continue;
              }

              // add up the result.
              ++sampleCount;
              us += tmp_us;
              sy += tmp_sy;
              wi += tmp_wi;
            }
          }

          // set the value.
          if (sampleCount > 0){
            double finalResult = (us + sy + wi)/sampleCount;
            logger.info("Cpu usage result  - " + finalResult );
            stats.setCpuUpsage(finalResult);
          }
        }
      }
      catch (Exception ex){
        logger.error("failed fetch system memory info as exception is captured when fetching result from bash call.");
      }
    } else {
        logger.error("failed fetch system memory info as 'bash' or 'free' can't be found on the current system.");
    }
  }

  // TO-DO - decide if we need to populate the remaining space and priority info.
}
