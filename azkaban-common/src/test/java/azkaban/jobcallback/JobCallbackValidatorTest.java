package azkaban.jobcallback;

import static azkaban.jobcallback.JobCallbackConstants.DEFAULT_MAX_CALLBACK_COUNT;
import static azkaban.jobcallback.JobCallbackConstants.MAX_CALLBACK_COUNT_PROPERTY_KEY;
import static azkaban.jobcallback.JobCallbackConstants.MAX_POST_BODY_LENGTH_PROPERTY_KEY;

import azkaban.utils.Props;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobCallbackValidatorTest {

  private Props serverProps;

  @Before
  public void setup() {
    this.serverProps = new Props();
    this.serverProps
        .put(MAX_CALLBACK_COUNT_PROPERTY_KEY, DEFAULT_MAX_CALLBACK_COUNT);
  }

  @Test
  public void noJobCallbackProps() {
    final Props jobProps = new Props();
    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void sequenceStartWithZeroProps() {
    final Props jobProps = new Props();
    final Set<String> errors = new HashSet<>();

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".0.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.COMPLETED.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void oneGetJobCallback() {
    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void onePostJobCallback() {
    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.body",
        "doc:id");

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(1, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void multiplePostJobCallbacks() {
    final Props jobProps = new Props();
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

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void noPostBodyJobCallback() {
    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(1, errors.size());
    System.out.println(errors);
  }

  @Test
  public void multipleGetJobCallbacks() {
    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void multipleGetJobCallbackWithGap() {
    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".2.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".2.url",
        "http://www.linkedin.com");

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(2, JobCallbackValidator.validate("bogusJob",
        this.serverProps, jobProps, errors));

    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void postBodyLengthTooLargeTest() {

    final Props jobProps = new Props();
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url",
        "http://www.linkedin.com");

    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);

    final String postBodyValue = "abcdefghijklmnopqrstuvwxyz";

    final int postBodyLength = 20;
    Assert.assertTrue(postBodyValue.length() > postBodyLength);
    jobProps.put("job.notification."
            + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.body",
        postBodyValue);

    final Props localServerProps = new Props();
    localServerProps.put(MAX_POST_BODY_LENGTH_PROPERTY_KEY, postBodyLength);

    final Set<String> errors = new HashSet<>();

    Assert.assertEquals(0, JobCallbackValidator.validate("bogusJob",
        localServerProps, jobProps, errors));

    System.out.println(errors);
    Assert.assertEquals(1, errors.size());

  }
}
