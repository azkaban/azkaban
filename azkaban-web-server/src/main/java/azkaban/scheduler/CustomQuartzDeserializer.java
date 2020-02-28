package azkaban.scheduler;

import azkaban.project.CronSchedule;
import azkaban.server.AzkabanServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.SchedulerStateRecord;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;
import org.quartz.impl.jdbcjobstore.TriggerPersistenceDelegate;
import org.quartz.impl.jdbcjobstore.TriggerStatus;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This class exists solely to fix https://github.com/azkaban/azkaban/issues/2461
 *
 * It is a necessary evil to help a minority of users affected by a bug related
 * to serialization and database corruption. It intercepts selectJobDetail
 * and provides an overridden implementation of getObjectFromBlob which uses a
 * custom object deserializer which ignores a specific SID of CronSchedule, as
 * specified by FixedObjectInputStream::shouldFix. All other methods of
 * StdJDBCDelegate are passed directly to the wrapped implementation.
 *
 * This class can be wired in automatically by QuartzHacker. It can also be
 * enabled explicitly by setting the following configuration keys:
 * 1. org.quartz.jobStore.driverDelegateClass=azkaban.scheduler.CustomQuartzDeserializer
 * 2. azkaban.quartz.jobStore.driverDelegateClass=[Database implementation to be wrapped]
 */
public class CustomQuartzDeserializer extends StdJDBCDelegate {

  public static final long BAD_CRON_SID = -1330280892166841228L;
  private static final Logger logger = LoggerFactory.getLogger(CustomQuartzDeserializer.class);
  private final StdJDBCDelegate inner;
  private final boolean delegateInitialization;

  public CustomQuartzDeserializer(final StdJDBCDelegate inner) {
    this.inner = inner;
    this.delegateInitialization = false;
  }

  public CustomQuartzDeserializer()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
    final String innerDelegate = AzkabanServer.getAzkabanProperties()
        .getString("azkaban.quartz.jobStore.driverDelegateClass");
    this.inner = (StdJDBCDelegate) Class.forName(innerDelegate).getDeclaredConstructor()
        .newInstance();
    this.delegateInitialization = true;
  }

  static <T> T invoke(final StdJDBCDelegate inner, final Class<T> tClass, final String methodName,
      final Object... args) {
    try {
      final Method method = findMethod(inner.getClass(), methodName);
      return tClass.cast(method.invoke(inner, args));
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (final InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static Method findMethod(final Class<?> aClass, final String name) {
    final Optional<Method> optional = Arrays.stream(aClass.getDeclaredMethods())
        .filter(x -> x.getName().equals(name)).findFirst();
    if (!optional.isPresent()) {
      final Class<?> superclass = aClass.getSuperclass();
      if (superclass != null) {
        final Method method = findMethod(superclass, name);
        if (method != null) {
          return method;
        }
      }
      logger.warn(String.format("No method named %s in %s", name, aClass));
      return null;
    }
    return optional.get();
  }

  @Override
  public void initialize(final Logger logger, final String tablePrefix, final String schedName,
      final String instanceId, final ClassLoadHelper classLoadHelper,
      final boolean useProperties, final String initString)
      throws NoSuchDelegateException {
    if (delegateInitialization) {
      inner.initialize(logger, tablePrefix, schedName, instanceId, classLoadHelper, useProperties,
          initString);
    }
    super.initialize(logger, tablePrefix, schedName, instanceId, classLoadHelper, useProperties,
        initString);
  }

  @Override
  public void addTriggerPersistenceDelegate(final TriggerPersistenceDelegate delegate) {
    inner.addTriggerPersistenceDelegate(delegate);
  }

  @Override
  public TriggerPersistenceDelegate findTriggerPersistenceDelegate(final OperableTrigger trigger) {
    return inner.findTriggerPersistenceDelegate(trigger);
  }

  @Override
  public TriggerPersistenceDelegate findTriggerPersistenceDelegate(final String discriminator) {
    return inner.findTriggerPersistenceDelegate(discriminator);
  }

  @Override
  public int updateTriggerStatesFromOtherStates(final Connection conn, final String newState,
      final String oldState1, final String oldState2) throws SQLException {
    return inner.updateTriggerStatesFromOtherStates(conn, newState, oldState1, oldState2);
  }

  @Override
  public List<TriggerKey> selectMisfiredTriggers(final Connection conn, final long ts)
      throws SQLException {
    return inner.selectMisfiredTriggers(conn, ts);
  }

  @Override
  public List<TriggerKey> selectTriggersInState(final Connection conn, final String state)
      throws SQLException {
    return inner.selectTriggersInState(conn, state);
  }

  @Override
  public List<TriggerKey> selectMisfiredTriggersInState(final Connection conn, final String state,
      final long ts) throws SQLException {
    return inner.selectMisfiredTriggersInState(conn, state, ts);
  }

  @Override
  public boolean hasMisfiredTriggersInState(final Connection conn, final String state1,
      final long ts, final int count, final List<TriggerKey> resultList) throws SQLException {
    return inner.hasMisfiredTriggersInState(conn, state1, ts, count, resultList);
  }

  @Override
  public int countMisfiredTriggersInState(final Connection conn, final String state1, final long ts)
      throws SQLException {
    return inner.countMisfiredTriggersInState(conn, state1, ts);
  }

  @Override
  public List<TriggerKey> selectMisfiredTriggersInGroupInState(final Connection conn,
      final String groupName, final String state, final long ts) throws SQLException {
    return inner.selectMisfiredTriggersInGroupInState(conn, groupName, state, ts);
  }

  @Override
  public List<OperableTrigger> selectTriggersForRecoveringJobs(final Connection conn)
      throws SQLException, IOException, ClassNotFoundException {
    return inner.selectTriggersForRecoveringJobs(conn);
  }

  @Override
  public int deleteFiredTriggers(final Connection conn) throws SQLException {
    return inner.deleteFiredTriggers(conn);
  }

  @Override
  public int deleteFiredTriggers(final Connection conn, final String theInstanceId)
      throws SQLException {
    return inner.deleteFiredTriggers(conn, theInstanceId);
  }

  @Override
  public void clearData(final Connection conn) throws SQLException {
    inner.clearData(conn);
  }

  @Override
  public int insertJobDetail(final Connection conn, final JobDetail job)
      throws IOException, SQLException {
    return inner.insertJobDetail(conn, job);
  }

  @Override
  public int updateJobDetail(final Connection conn, final JobDetail job)
      throws IOException, SQLException {
    return inner.updateJobDetail(conn, job);
  }

  @Override
  public List<TriggerKey> selectTriggerKeysForJob(final Connection conn, final JobKey jobKey)
      throws SQLException {
    return inner.selectTriggerKeysForJob(conn, jobKey);
  }

  @Override
  public int deleteJobDetail(final Connection conn, final JobKey jobKey) throws SQLException {
    return inner.deleteJobDetail(conn, jobKey);
  }

  @Override
  public boolean isJobNonConcurrent(final Connection conn, final JobKey jobKey)
      throws SQLException {
    return inner.isJobNonConcurrent(conn, jobKey);
  }

  @Override
  public boolean jobExists(final Connection conn, final JobKey jobKey) throws SQLException {
    return inner.jobExists(conn, jobKey);
  }

  @Override
  public int updateJobData(final Connection conn, final JobDetail job)
      throws IOException, SQLException {
    return inner.updateJobData(conn, job);
  }

  @Override
  public int selectNumJobs(final Connection conn) throws SQLException {
    return inner.selectNumJobs(conn);
  }

  @Override
  public List<String> selectJobGroups(final Connection conn) throws SQLException {
    return inner.selectJobGroups(conn);
  }

  @Override
  public Set<JobKey> selectJobsInGroup(final Connection conn, final GroupMatcher<JobKey> matcher)
      throws SQLException {
    return inner.selectJobsInGroup(conn, matcher);
  }

  @Override
  public int insertTrigger(final Connection conn, final OperableTrigger trigger, final String state,
      final JobDetail jobDetail) throws SQLException, IOException {
    return inner.insertTrigger(conn, trigger, state, jobDetail);
  }

  @Override
  public int insertBlobTrigger(final Connection conn, final OperableTrigger trigger)
      throws SQLException, IOException {
    return inner.insertBlobTrigger(conn, trigger);
  }

  @Override
  public int updateTrigger(final Connection conn, final OperableTrigger trigger, final String state,
      final JobDetail jobDetail) throws SQLException, IOException {
    return inner.updateTrigger(conn, trigger, state, jobDetail);
  }

  @Override
  public int updateBlobTrigger(final Connection conn, final OperableTrigger trigger)
      throws SQLException, IOException {
    return inner.updateBlobTrigger(conn, trigger);
  }

  @Override
  public boolean triggerExists(final Connection conn, final TriggerKey triggerKey)
      throws SQLException {
    return inner.triggerExists(conn, triggerKey);
  }

  @Override
  public int updateTriggerState(final Connection conn, final TriggerKey triggerKey,
      final String state)
      throws SQLException {
    return inner.updateTriggerState(conn, triggerKey, state);
  }

  @Override
  public int updateTriggerStateFromOtherStates(final Connection conn, final TriggerKey triggerKey,
      final String newState, final String oldState1, final String oldState2, final String oldState3)
      throws SQLException {
    return inner.updateTriggerStateFromOtherStates(conn, triggerKey, newState, oldState1, oldState2,
        oldState3);
  }

  @Override
  public int updateTriggerGroupStateFromOtherStates(final Connection conn,
      final GroupMatcher<TriggerKey> matcher, final String newState, final String oldState1,
      final String oldState2, final String oldState3) throws SQLException {
    return inner
        .updateTriggerGroupStateFromOtherStates(conn, matcher, newState, oldState1, oldState2,
            oldState3);
  }

  @Override
  public int updateTriggerStateFromOtherState(final Connection conn, final TriggerKey triggerKey,
      final String newState, final String oldState) throws SQLException {
    return inner.updateTriggerStateFromOtherState(conn, triggerKey, newState, oldState);
  }

  @Override
  public int updateTriggerGroupStateFromOtherState(final Connection conn,
      final GroupMatcher<TriggerKey> matcher, final String newState, final String oldState)
      throws SQLException {
    return inner.updateTriggerGroupStateFromOtherState(conn, matcher, newState, oldState);
  }

  @Override
  public int updateTriggerStatesForJob(final Connection conn, final JobKey jobKey,
      final String state) throws SQLException {
    return inner.updateTriggerStatesForJob(conn, jobKey, state);
  }

  @Override
  public int updateTriggerStatesForJobFromOtherState(final Connection conn, final JobKey jobKey,
      final String state, final String oldState) throws SQLException {
    return inner.updateTriggerStatesForJobFromOtherState(conn, jobKey, state, oldState);
  }

  @Override
  public int deleteBlobTrigger(final Connection conn, final TriggerKey triggerKey)
      throws SQLException {
    return inner.deleteBlobTrigger(conn, triggerKey);
  }

  @Override
  public int deleteTrigger(final Connection conn, final TriggerKey triggerKey) throws SQLException {
    return inner.deleteTrigger(conn, triggerKey);
  }

  @Override
  public int selectNumTriggersForJob(final Connection conn, final JobKey jobKey)
      throws SQLException {
    return inner.selectNumTriggersForJob(conn, jobKey);
  }

  @Override
  public JobDetail selectJobForTrigger(final Connection conn, final ClassLoadHelper loadHelper,
      final TriggerKey triggerKey) throws ClassNotFoundException, SQLException {
    return inner.selectJobForTrigger(conn, loadHelper, triggerKey);
  }

  @Override
  public JobDetail selectJobForTrigger(final Connection conn, final ClassLoadHelper loadHelper,
      final TriggerKey triggerKey, final boolean loadJobClass)
      throws ClassNotFoundException, SQLException {
    return inner.selectJobForTrigger(conn, loadHelper, triggerKey, loadJobClass);
  }

  @Override
  public List<OperableTrigger> selectTriggersForJob(final Connection conn, final JobKey jobKey)
      throws SQLException, ClassNotFoundException, IOException, JobPersistenceException {
    return inner.selectTriggersForJob(conn, jobKey);
  }

  @Override
  public List<OperableTrigger> selectTriggersForCalendar(final Connection conn,
      final String calName)
      throws SQLException, ClassNotFoundException, IOException, JobPersistenceException {
    return inner.selectTriggersForCalendar(conn, calName);
  }

  @Override
  public OperableTrigger selectTrigger(final Connection conn, final TriggerKey triggerKey)
      throws SQLException, ClassNotFoundException, IOException, JobPersistenceException {
    return inner.selectTrigger(conn, triggerKey);
  }

  @Override
  public JobDataMap selectTriggerJobDataMap(final Connection conn, final String triggerName,
      final String groupName) throws SQLException, ClassNotFoundException, IOException {
    return inner.selectTriggerJobDataMap(conn, triggerName, groupName);
  }

  @Override
  public String selectTriggerState(final Connection conn, final TriggerKey triggerKey)
      throws SQLException {
    return inner.selectTriggerState(conn, triggerKey);
  }

  @Override
  public TriggerStatus selectTriggerStatus(final Connection conn, final TriggerKey triggerKey)
      throws SQLException {
    return inner.selectTriggerStatus(conn, triggerKey);
  }

  @Override
  public int selectNumTriggers(final Connection conn) throws SQLException {
    return inner.selectNumTriggers(conn);
  }

  @Override
  public List<String> selectTriggerGroups(final Connection conn) throws SQLException {
    return inner.selectTriggerGroups(conn);
  }

  @Override
  public List<String> selectTriggerGroups(final Connection conn,
      final GroupMatcher<TriggerKey> matcher) throws SQLException {
    return inner.selectTriggerGroups(conn, matcher);
  }

  @Override
  public Set<TriggerKey> selectTriggersInGroup(final Connection conn,
      final GroupMatcher<TriggerKey> matcher) throws SQLException {
    return inner.selectTriggersInGroup(conn, matcher);
  }

  @Override
  public int insertPausedTriggerGroup(final Connection conn, final String groupName)
      throws SQLException {
    return inner.insertPausedTriggerGroup(conn, groupName);
  }

  @Override
  public int deletePausedTriggerGroup(final Connection conn, final String groupName)
      throws SQLException {
    return inner.deletePausedTriggerGroup(conn, groupName);
  }

  @Override
  public int deletePausedTriggerGroup(final Connection conn, final GroupMatcher<TriggerKey> matcher)
      throws SQLException {
    return inner.deletePausedTriggerGroup(conn, matcher);
  }

  @Override
  public int deleteAllPausedTriggerGroups(final Connection conn) throws SQLException {
    return inner.deleteAllPausedTriggerGroups(conn);
  }

  @Override
  public boolean isTriggerGroupPaused(final Connection conn, final String groupName)
      throws SQLException {
    return inner.isTriggerGroupPaused(conn, groupName);
  }

  @Override
  public boolean isExistingTriggerGroup(final Connection conn, final String groupName)
      throws SQLException {
    return inner.isExistingTriggerGroup(conn, groupName);
  }

  @Override
  public int insertCalendar(final Connection conn, final String calendarName,
      final Calendar calendar)
      throws IOException, SQLException {
    return inner.insertCalendar(conn, calendarName, calendar);
  }

  @Override
  public int updateCalendar(final Connection conn, final String calendarName,
      final Calendar calendar)
      throws IOException, SQLException {
    return inner.updateCalendar(conn, calendarName, calendar);
  }

  @Override
  public boolean calendarExists(final Connection conn, final String calendarName)
      throws SQLException {
    return inner.calendarExists(conn, calendarName);
  }

  @Override
  public Calendar selectCalendar(final Connection conn, final String calendarName)
      throws ClassNotFoundException, IOException, SQLException {
    return inner.selectCalendar(conn, calendarName);
  }

  @Override
  public boolean calendarIsReferenced(final Connection conn, final String calendarName)
      throws SQLException {
    return inner.calendarIsReferenced(conn, calendarName);
  }

  @Override
  public int deleteCalendar(final Connection conn, final String calendarName) throws SQLException {
    return inner.deleteCalendar(conn, calendarName);
  }

  @Override
  public int selectNumCalendars(final Connection conn) throws SQLException {
    return inner.selectNumCalendars(conn);
  }

  @Override
  public List<String> selectCalendars(final Connection conn) throws SQLException {
    return inner.selectCalendars(conn);
  }

  @Override
  public long selectNextFireTime(final Connection conn) throws SQLException {
    return inner.selectNextFireTime(conn);
  }

  @Override
  public TriggerKey selectTriggerForFireTime(final Connection conn, final long fireTime)
      throws SQLException {
    return inner.selectTriggerForFireTime(conn, fireTime);
  }

  @Override
  public List<TriggerKey> selectTriggerToAcquire(final Connection conn, final long noLaterThan,
      final long noEarlierThan) throws SQLException {
    return inner.selectTriggerToAcquire(conn, noLaterThan, noEarlierThan);
  }

  @Override
  public List<TriggerKey> selectTriggerToAcquire(final Connection conn, final long noLaterThan,
      final long noEarlierThan, final int maxCount) throws SQLException {
    return inner.selectTriggerToAcquire(conn, noLaterThan, noEarlierThan, maxCount);
  }

  @Override
  public int insertFiredTrigger(final Connection conn, final OperableTrigger trigger,
      final String state, final JobDetail job) throws SQLException {
    return inner.insertFiredTrigger(conn, trigger, state, job);
  }

  @Override
  public int updateFiredTrigger(final Connection conn, final OperableTrigger trigger,
      final String state, final JobDetail job) throws SQLException {
    return inner.updateFiredTrigger(conn, trigger, state, job);
  }

  @Override
  public List<FiredTriggerRecord> selectFiredTriggerRecords(final Connection conn,
      final String triggerName,
      final String groupName) throws SQLException {
    return inner.selectFiredTriggerRecords(conn, triggerName, groupName);
  }

  @Override
  public List<FiredTriggerRecord> selectFiredTriggerRecordsByJob(final Connection conn,
      final String jobName,
      final String groupName) throws SQLException {
    return inner.selectFiredTriggerRecordsByJob(conn, jobName, groupName);
  }

  @Override
  public List<FiredTriggerRecord> selectInstancesFiredTriggerRecords(final Connection conn,
      final String instanceName) throws SQLException {
    return inner.selectInstancesFiredTriggerRecords(conn, instanceName);
  }

  @Override
  public Set<String> selectFiredTriggerInstanceNames(final Connection conn) throws SQLException {
    return inner.selectFiredTriggerInstanceNames(conn);
  }

  @Override
  public int deleteFiredTrigger(final Connection conn, final String entryId) throws SQLException {
    return inner.deleteFiredTrigger(conn, entryId);
  }

  @Override
  public int selectJobExecutionCount(final Connection conn, final JobKey jobKey)
      throws SQLException {
    return inner.selectJobExecutionCount(conn, jobKey);
  }

  @Override
  public int insertSchedulerState(final Connection conn, final String theInstanceId,
      final long checkInTime, final long interval) throws SQLException {
    return inner.insertSchedulerState(conn, theInstanceId, checkInTime, interval);
  }

  @Override
  public int deleteSchedulerState(final Connection conn, final String theInstanceId)
      throws SQLException {
    return inner.deleteSchedulerState(conn, theInstanceId);
  }

  @Override
  public int updateSchedulerState(final Connection conn, final String theInstanceId,
      final long checkInTime) throws SQLException {
    return inner.updateSchedulerState(conn, theInstanceId, checkInTime);
  }

  @Override
  public List<SchedulerStateRecord> selectSchedulerStateRecords(final Connection conn,
      final String theInstanceId) throws SQLException {
    return inner.selectSchedulerStateRecords(conn, theInstanceId);
  }

  @Override
  public Set<String> selectPausedTriggerGroups(final Connection conn) throws SQLException {
    return inner.selectPausedTriggerGroups(conn);
  }

  @Override
  protected Object getObjectFromBlob(final ResultSet rs, final String colName)
      throws ClassNotFoundException, IOException, SQLException {
    final byte[] bytes = rs.getBytes(colName);
    if (bytes != null && bytes.length != 0) {
      final InputStream binaryInput = new ByteArrayInputStream(bytes);
      final ObjectInputStream in = new FixedObjectInputStream(binaryInput);
      try {
        return in.readObject();
      } finally {
        in.close();
      }
    }
    return null;
  }

  @Override
  protected Object getJobDataFromBlob(final ResultSet rs, final String colName) {
    return invoke(inner, Object.class, "getJobDataFromBlob", rs, colName);
  }

  private static class FixedObjectInputStream extends ObjectInputStream {

    public FixedObjectInputStream(final InputStream binaryInput) throws IOException {
      super(binaryInput);
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
      // From https://stackoverflow.com/questions/795470/how-to-deserialize-an-object-persisted-in-a-db-now-when-the-object-has-different/796589#796589
      final ObjectStreamClass resultClassDescriptor = super.readClassDescriptor();
      final Class localClass = Class.forName(resultClassDescriptor.getName());
      if (localClass == null) {
        logger.info("No local class for " + resultClassDescriptor.getName());
        return resultClassDescriptor;
      }
      final ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
      if (localClassDescriptor != null) {
        final long localSUID = localClassDescriptor.getSerialVersionUID();
        final long streamSUID = resultClassDescriptor.getSerialVersionUID();
        if (streamSUID != localSUID) {
          if (shouldFix(localClass, streamSUID)) {
            return localClassDescriptor;
          }
        }
      }
      return resultClassDescriptor;
    }

    protected boolean shouldFix(final Class aClass, final long sid) {
      return aClass == CronSchedule.class && sid == BAD_CRON_SID;
    }

  }
}
