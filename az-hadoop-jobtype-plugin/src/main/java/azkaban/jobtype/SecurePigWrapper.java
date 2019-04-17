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
package azkaban.jobtype;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.security.commons.SecurityUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;


public class SecurePigWrapper {

  public static final String OBTAIN_BINARY_TOKEN = "obtain.binary.token";
  public static final String MAPREDUCE_JOB_CREDENTIALS_BINARY =
      "mapreduce.job.credentials.binary";

  public static void main(final String[] args) throws IOException,
      InterruptedException {
    final Logger logger = Logger.getRootLogger();
    final Properties p = System.getProperties();
    final Configuration conf = new Configuration();

    SecurityUtils.getProxiedUser(p, logger, conf).doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            prefetchToken();
            org.apache.pig.Main.main(args);
            return null;
          }

          // For Pig jobs that need to do extra communication with the
          // JobTracker, it's necessary to pre-fetch a token and include it in
          // the credentials cache
          private void prefetchToken() throws InterruptedException, IOException {
            String shouldPrefetch = p.getProperty(OBTAIN_BINARY_TOKEN);
            if (shouldPrefetch != null && shouldPrefetch.equals("true")) {
              logger.info("Pre-fetching token");
              Job job =
                  new Job(conf, "totally phony, extremely fake, not real job");

              JobConf jc = new JobConf(conf);
              JobClient jobClient = new JobClient(jc);
              logger.info("Pre-fetching: Got new JobClient: " + jc);
              Token<DelegationTokenIdentifier> mrdt =
                  jobClient.getDelegationToken(new Text("hi"));
              job.getCredentials().addToken(new Text("howdy"), mrdt);

              File temp = File.createTempFile("mr-azkaban", ".token");
              temp.deleteOnExit();

              FileOutputStream fos = null;
              DataOutputStream dos = null;
              try {
                fos = new FileOutputStream(temp);
                dos = new DataOutputStream(fos);
                job.getCredentials().writeTokenStorageToStream(dos);
              } finally {
                if (dos != null) {
                  dos.close();
                }
                if (fos != null) {
                  fos.close();
                }
              }
              logger.info("Setting " + MAPREDUCE_JOB_CREDENTIALS_BINARY
                  + " to " + temp.getAbsolutePath());
              System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY,
                  temp.getAbsolutePath());
            } else {
              logger.info("Not pre-fetching token");
            }
          }
        });
  }
}
