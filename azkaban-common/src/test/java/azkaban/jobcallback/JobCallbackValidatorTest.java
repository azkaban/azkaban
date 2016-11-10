package azkaban.jobcallback;

import static azkaban.jobcallback.JobCallbackConstants.DEFAULT_MAX_CALLBACK_COUNT;
import static azkaban.jobcallback.JobCallbackConstants.MAX_CALLBACK_COUNT_PROPERTY_KEY;
import static azkaban.jobcallback.JobCallbackConstants.MAX_POST_BODY_LENGTH_PROPERTY_KEY;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Props;

public class JobCallbackValidatorTest {
  private Props serverProps;

  @Before
  public void setup() {
    serverProps = new Props();
    serverProps
        .put(MAX_CALLBACK_COUNT_PROPERTY_KEY, DEFAULT_MAX_CALLBACK_COUNT);
  }

  @Test
  public void noJobCallbackProps() {
    Props jobProps = new Props();
    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void sequenceStartWithZeroProps() {
    Props jobProps = new Props();
    Set<String> errors = new HashSet<String>();

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".0.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.COMPLETED.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void oneGetJobCallback() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void onePostJobCallback() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.body",
        "doc:id");

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void multiplePostJobCallbacks() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.body",
        "doc:id");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".2.url",
        "http://www.linkedin2.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".2.method",
        JobCallbackConstants.HTTP_POST);

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".2.body",
        "doc2:id");

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void noPostBodyJobCallback() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(1, errors.size());
    System.out.println(errors);
  }

  @Test
  public void multipleGetJobCallbacks() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void multipleGetJobCallbackWithGap() {
    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".2.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".2.url",
        "http://www.linkedin.com");

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void postBodyLengthTooLargeTest() {

    Props jobProps = new Props();
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    String postBodyValue = "abcdefghijklmnopqrstuvwxyz";

    int postBodyLength = 20;
    Assert.assertTrue(postBodyValue.length() > postBodyLength);
    jobProps.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.body",
        postBodyValue);

    Props localServerProps = new Props();
    localServerProps.put(MAX_POST_BODY_LENGTH_PROPERTY_KEY, postBodyLength);

    Set<String> errors = new HashSet<String>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        localServerProps, jobProps, errors));

    System.out.println(errors);
    Assert.assertEquals(1, errors.size());

  }
}
