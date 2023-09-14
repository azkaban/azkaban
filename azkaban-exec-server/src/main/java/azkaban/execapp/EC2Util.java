package azkaban.execapp;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesRequest;
import com.amazonaws.services.autoscaling.model.SetInstanceProtectionRequest;
import com.amazonaws.util.EC2MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class EC2Util {

  private static final Logger LOGGER = LoggerFactory.getLogger(EC2Util.class);

  private static String instanceId;
  private static String autoScalingGroupName;
  private static AmazonAutoScaling asg;
  private static final AtomicBoolean localState = new AtomicBoolean(false);
  private static final AtomicBoolean awsState = new AtomicBoolean(false);
  private static volatile Thread syncThread;
  private static final Object syncThreadLock = new Object();

  public static void markASGProtection(boolean protectedFromScaleIn) {
    try {
      if(localState.get() == protectedFromScaleIn) return;
      initializeBackgroundThread();
      localState.set(protectedFromScaleIn);
      attemptUpdate();
    } catch (Throwable e) {
      LOGGER.warn("Error setting ASG protection", e);
    }
  }

  private static boolean setASGProtection(boolean protectedFromScaleIn) {
    if(null == instanceId) {
      instanceId = EC2MetadataUtils.getInstanceId();
    }
    if(null == asg) {
      asg = AmazonAutoScalingClientBuilder.standard().build();
    }
    if(null == autoScalingGroupName) {
      autoScalingGroupName = asg.describeAutoScalingInstances(
              new DescribeAutoScalingInstancesRequest().withInstanceIds(instanceId))
              .getAutoScalingInstances().get(0).getAutoScalingGroupName();
    }
    asg.setInstanceProtection(new SetInstanceProtectionRequest()
            .withInstanceIds(instanceId)
            .withAutoScalingGroupName(autoScalingGroupName)
            .withProtectedFromScaleIn(protectedFromScaleIn));
    LOGGER.info(String.format("Set ASG protection for %s to %s", autoScalingGroupName, protectedFromScaleIn));
    return protectedFromScaleIn;
  }

  private static void initializeBackgroundThread() {
    if(null==syncThread) {
      synchronized (syncThreadLock) {
        if(null==syncThread) {
          syncThread = new Thread(()->{
            while(true) {
              attemptUpdate();
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });
          syncThread.setDaemon(true);
          syncThread.setName("EC2 Scale-In Protection");
          syncThread.start();
        }
      }
    }
  }

  private static void attemptUpdate() {
    synchronized (syncThreadLock) {
      try {
        if(localState.get() != awsState.get()) {
          awsState.set(setASGProtection(localState.get()));
        }
      } catch (Throwable e) {
        LOGGER.warn("Error synchronizing EC2 scale-in protection", e);
      }
    }
  }

  public static void onIdle() {
    markASGProtection(false);
  }

  public static void onBusy() {
    markASGProtection(true);
  }
}
