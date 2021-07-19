package azkaban.scheduler;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import org.quartz.Scheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.jdbcjobstore.DriverDelegate;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This class exists solely to fix https://github.com/azkaban/azkaban/issues/2461
 *
 * It is a necessary evil to help a minority of users affected by a bug related
 * to serialization and database corruption.
 *
 * It handles unconfigured cases where the bug is encountered and allows for automated
 * remediation measures when QuartzScheduler::enableSerializationHack is triggered
 * by appropriate exceptions, identified by QuartzScheduler::isSerializationBug.
 */
public class QuartzHacker {

  private static final Logger logger = LoggerFactory.getLogger(QuartzHacker.class);

  static Scheduler fix(final Scheduler scheduler) {
    org.quartz.core.QuartzScheduler quartzScheduler = getPrivateField(scheduler, "sched",
        org.quartz.core.QuartzScheduler.class);
    if (quartzScheduler == null) {
      logger.info("No sched field in scheduler");
      return null;
    }
    QuartzSchedulerResources resources = getPrivateField(quartzScheduler, "resources",
        QuartzSchedulerResources.class);
    if (resources == null) {
      logger.info("No resources field in quartzScheduler");
      return null;
    }
    JobStore jobStore = resources.getJobStore();
    DriverDelegate delegate = getPrivateField(jobStore, "delegate", DriverDelegate.class);
    if (delegate == null) {
      logger.info("No resources delegate in jobStore");
      return null;
    }
    if (!(delegate instanceof StdJDBCDelegate)) {
      logger.info(String
          .format("Class of delegate is not StdJDBCDelegate (%s)", delegate.getClass().getName()));
      return null; // Do not re-wrap!
    }
    if (delegate instanceof CustomQuartzDeserializer) {
      logger.info("Class of delegate is already CustomQuartzDeserializer");
      return null; // Do not re-wrap!
    }
    StdJDBCDelegate inner = (StdJDBCDelegate) delegate;
    CustomQuartzDeserializer value = new CustomQuartzDeserializer(inner);
    if (!setPrivateField(jobStore, "delegate", value)) {
      logger.info("Cannot set delegate of jobStore");
      return null;
    }
    initialize(value, inner);
    if (!setPrivateField(quartzScheduler, "initialStart", null)) {
      logger.info("Cannot reset initialStart of quartzScheduler");
      return null;
    }
    logger.warn(String.format(
        "Quartz internals hacked to fix object serialization issue. (See https://github.com/azkaban/azkaban/issues/2461)\n"
            +
            "To statically configure and remove this warning, please change your configuration to include:\n"
            +
            "\torg.quartz.jobStore.driverDelegateClass=%s\n" +
            "\tazkaban.quartz.jobStore.driverDelegateClass=%s",
        CustomQuartzDeserializer.class.getName(), inner.getClass().getName()));
    return scheduler;
  }

  private static <T> boolean setPrivateField(final Object obj, final String name, final T value) {
    Field field = findField(obj.getClass(), name);
    if (field == null) {
      return false;
    }
    field.setAccessible(true);
    try {
      field.set(obj, value);
      return true;
    } catch (IllegalAccessException e) {
      logger.warn("Error accessing sched", e);
      return false;
    }
  }

  static <T> T getPrivateField(final Object obj, final String name, final Class<T> tClass) {
    Field field = findField(obj.getClass(), name);
    if (field == null) {
      return null;
    }
    field.setAccessible(true);
    try {
      return tClass.cast(field.get(obj));
    } catch (IllegalAccessException e) {
      logger.warn("Error accessing sched", e);
      return null;
    }
  }

  private static Field findField(final Class<?> aClass, final String name) {
    Optional<Field> sched = Arrays.stream(aClass.getDeclaredFields())
        .filter(field -> field.getName().equals(name)).findFirst();
    if (!sched.isPresent()) {
      Class<?> superclass = aClass.getSuperclass();
      if (superclass != null) {
        Field field = findField(superclass, name);
        if (null != field) {
          return field;
        }
      }
      logger.warn(String.format("No field named %s in %s", name, aClass));
      return null;
    }
    return sched.get();
  }

  public static void initialize(final StdJDBCDelegate target, final StdJDBCDelegate inner) {
    try {
      target.initialize(
          getPrivateField(inner, "logger", Logger.class),
          getPrivateField(inner, "tablePrefix", String.class),
          getPrivateField(inner, "schedName", String.class),
          getPrivateField(inner, "instanceId", String.class),
          getPrivateField(inner, "classLoadHelper", ClassLoadHelper.class),
          getPrivateField(inner, "useProperties", Boolean.class),
          ""
      );
    } catch (NoSuchDelegateException e) {
      throw new RuntimeException(e);
    }
  }
}
