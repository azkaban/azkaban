package azkaban.jobtype;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import azkaban.utils.Props;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.beeline.BeeLine;
import org.apache.log4j.Logger;

public class HadoopSecureBeelineWrapper {

  private static final Logger logger = Logger.getRootLogger();
  private static String hiveScript;
  
  public static void main(final String[] args) throws IOException, InterruptedException {
    final Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    hiveScript = jobProps.getProperty("hive.script");

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      final String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      System.setProperty("mapreduce.job.credentials.binary", tokenFile);
      final UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runBeeline(args);
          return null;
        }
      });
    } else {
      runBeeline(args);
    }
  }

  private static void runBeeline(final String[] args) throws IOException {
    final BeeLine beeline = new BeeLine();
    final int status = beeline.begin(args, null);
    beeline.close();
    if (status != 0) {
      System.exit(0);
    }
  }
}
