/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.flowtrigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.db.DatabaseOperator;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.database.JdbcFlowTriggerInstanceLoaderImpl;
import azkaban.project.DirectoryYamlFlowLoader;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.FlowTrigger;
import azkaban.project.FlowTriggerDependency;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.JdbcProjectImplTest;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManager;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlowTriggerInstanceLoaderTest {

  private static final Logger logger = LoggerFactory.getLogger(FlowTriggerInstanceLoaderTest.class);
  private static final String test_project_zip_dir = "flowtriggeryamltest";
  private static final String test_flow_file = "flow_trigger.flow";
  private static final int project_id = 123;
  private static final String project_name = "test";
  private static final int project_version = 3;
  private static final String flow_id = "flow_trigger";
  private static final int flow_version = 1;
  private static final Props props = new Props();
  private static final String submitUser = "uploadUser1";
  private static DatabaseOperator dbOperator;
  private static ProjectLoader projLoader;
  private static FlowTrigger flowTrigger;
  private static FlowTriggerInstanceLoader triggerInstLoader;
  private static Project project;
  private static ProjectManager projManager;

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("SHUTDOWN");
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      logger.error("unable to destroy db", e);
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    dbOperator = Utils.initTestDB();
    projLoader = new JdbcProjectImpl(props, dbOperator);
    projManager = mock(ProjectManager.class);
    triggerInstLoader = new JdbcFlowTriggerInstanceLoaderImpl(dbOperator, projLoader, projManager);
    project = new Project(project_id, project_name);

    final DirectoryYamlFlowLoader yamlFlowLoader = new DirectoryYamlFlowLoader(new Props());
    yamlFlowLoader
        .loadProjectFlow(project, ExecutionsTestUtil.getFlowDir(test_project_zip_dir));
    project.setVersion(project_version);
    project.setFlows(yamlFlowLoader.getFlowMap());
    project.setLastModifiedUser(submitUser);

    final File flowFile = new File(JdbcProjectImplTest.class.getClassLoader().getResource
        (test_flow_file).getFile());

    when(projManager.getProject(project_id)).thenReturn(project);

    projLoader
        .uploadFlowFile(project_id, project_version, flowFile, flow_version);
    flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
  }


  private TriggerInstance createTriggerInstance(final FlowTrigger flowTrigger, final String flowId,
      final int flowVersion, final String submitUser, final Project project, final long startTime) {
    final String triggerInstId = UUID.randomUUID().toString();
    final List<DependencyInstance> depInstList = new ArrayList<>();
    for (final FlowTriggerDependency dep : flowTrigger.getDependencies()) {
      final String depName = dep.getName();
      final DependencyInstanceContext context = new TestDependencyInstanceContext(null, null, null);
      final Status status = Status.RUNNING;
      final CancellationCause cause = CancellationCause.NONE;
      final DependencyInstance depInst = new DependencyInstance(depName, startTime, 0, context,
          status, cause);
      depInstList.add(depInst);
    }

    final int flowExecId = Constants.UNASSIGNED_EXEC_ID;
    final TriggerInstance triggerInstance = new TriggerInstance(triggerInstId, flowTrigger,
        flowId, flowVersion, submitUser, depInstList, flowExecId, project);

    return triggerInstance;
  }


  @Test
  public void testUploadTriggerInstance() {
    final TriggerInstance expectedTriggerInst = this.createTriggerInstance(this.flowTrigger, this
        .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis());

    this.triggerInstLoader.uploadTriggerInstance(expectedTriggerInst);

    final TriggerInstance actualTriggerInst = this.triggerInstLoader
        .getTriggerInstanceById(expectedTriggerInst.getId());

    assertThat(expectedTriggerInst.getFlowTrigger().toString())
        .isEqualToIgnoringWhitespace(actualTriggerInst.getFlowTrigger().toString());

    assertThat(expectedTriggerInst).isEqualToIgnoringGivenFields(actualTriggerInst,
        "depInstances", "flowTrigger");

    assertThat(expectedTriggerInst.getDepInstances())
        .usingElementComparatorIgnoringFields("triggerInstance", "context")
        .containsAll(actualTriggerInst.getDepInstances())
        .hasSameSizeAs(actualTriggerInst.getDepInstances());
  }

  private void assertTriggerInstancesEqual(final TriggerInstance actual,
      final TriggerInstance expected, final boolean ignoreFlowTrigger) {
    if (!ignoreFlowTrigger) {
      if (actual.getFlowTrigger() != null && expected.getFlowTrigger() != null) {
        assertThat(actual.getFlowTrigger().toString())
            .isEqualToIgnoringWhitespace(expected.getFlowTrigger().toString());
      } else {
        assertThat(actual.getFlowTrigger()).isNull();
        assertThat(expected.getFlowTrigger()).isNull();
      }
    }

    assertThat(actual).isEqualToIgnoringGivenFields(expected, "depInstances", "flowTrigger");

    assertThat(actual.getDepInstances())
        .usingComparatorForElementFieldsWithType((d1, d2) -> {
          if (d1 == null && d2 == null) {
            return 0;
          } else if (d1 != null && d2 != null && d1.getTime() == d2.getTime()) {
            return 0;
          } else {
            return -1;
          }
        }, Date.class)
        .usingElementComparatorIgnoringFields("triggerInstance", "context")
        .containsExactlyInAnyOrder(expected.getDepInstances()
            .toArray(new DependencyInstance[expected.getDepInstances().size()]));
  }

  @Test
  public void testUpdateDependencyExecutionStatus() {
    final TriggerInstance expectedTriggerInst = this.createTriggerInstance(this.flowTrigger, this
        .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis());

    this.triggerInstLoader.uploadTriggerInstance(expectedTriggerInst);
    for (final DependencyInstance depInst : expectedTriggerInst.getDepInstances()) {
      depInst.setStatus(Status.CANCELLED);
      depInst.setEndTime(System.currentTimeMillis());
      depInst.setCancellationCause(CancellationCause.MANUAL);
      this.triggerInstLoader.updateDependencyExecutionStatus(depInst);
    }

    final TriggerInstance actualTriggerInst = this.triggerInstLoader
        .getTriggerInstanceById(expectedTriggerInst.getId());
    assertTriggerInstancesEqual(actualTriggerInst, expectedTriggerInst, false);
  }

  private void finalizeTriggerInstanceWithSuccess(final TriggerInstance triggerInst, final int
      associateFlowExecId) {
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      depInst.setStatus(Status.SUCCEEDED);
      depInst.getTriggerInstance().setFlowExecId(associateFlowExecId);
      depInst.setEndTime(System.currentTimeMillis());
    }
  }

  private void finalizeTriggerInstanceWithCancelled(final TriggerInstance triggerInst) {
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      depInst.setStatus(Status.CANCELLED);
      depInst.setCancellationCause(CancellationCause.TIMEOUT);
      depInst.setEndTime(System.currentTimeMillis());
    }
  }

  private void finalizeTriggerInstanceWithCancelling(final TriggerInstance triggerInst) {
    for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
      depInst.setStatus(Status.CANCELLING);
    }
  }

  private void shuffleAndUpload(final List<TriggerInstance> all) {
    final List<TriggerInstance> shuffled = new ArrayList<>(all);
    Collections.shuffle(shuffled);
    shuffled.forEach(triggerInst -> this.triggerInstLoader.uploadTriggerInstance(triggerInst));
  }

  @Test
  public void testGetIncompleteTriggerInstancesReturnsEmpty() {
    final List<TriggerInstance> all = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()));
      if (i <= 2) {
        finalizeTriggerInstanceWithCancelled(all.get(i));
      } else {
        finalizeTriggerInstanceWithSuccess(all.get(i), 1000);
      }
    }
    this.shuffleAndUpload(all);
    final List<TriggerInstance> actual = new ArrayList<>(this.triggerInstLoader
        .getIncompleteTriggerInstances());
    all.sort(Comparator.comparing(TriggerInstance::getId));
    actual.sort(Comparator.comparing(TriggerInstance::getId));

    assertThat(actual).isEmpty();
  }

  @Test
  public void testGetIncompleteTriggerInstances() {
    final List<TriggerInstance> allInstances = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      allInstances.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()));
    }

    finalizeTriggerInstanceWithCancelled(allInstances.get(0));
    finalizeTriggerInstanceWithSuccess(allInstances.get(1), 1000);
    // this trigger instance should still count as incomplete one since no flow execution has
    // been started
    finalizeTriggerInstanceWithSuccess(allInstances.get(2), -1);

    this.shuffleAndUpload(allInstances);

    final List<TriggerInstance> expected = allInstances.subList(2, allInstances.size());
    final List<TriggerInstance> actual = new ArrayList<>(this.triggerInstLoader
        .getIncompleteTriggerInstances());
    assertTwoTriggerInstanceListsEqual(actual, expected, false, false);
  }

  @Test
  public void testGetRunningTriggerInstancesReturnsEmpty() throws InterruptedException {
    final List<TriggerInstance> all = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()
          + i * 10000));
      finalizeTriggerInstanceWithSuccess(all.get(i), 1000);
    }

    this.shuffleAndUpload(all);

    final Collection<TriggerInstance> running = this.triggerInstLoader.getRunning();
    assertThat(running).isEmpty();
  }

  @Test
  public void testGetRunningTriggerInstances() throws InterruptedException {
    final List<TriggerInstance> all = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()
          + i * 10000));
      if (i <= 3) {
        finalizeTriggerInstanceWithCancelled(all.get(i));
      } else if (i <= 6) {
        finalizeTriggerInstanceWithSuccess(all.get(i), 1000);
      } else if (i <= 9) {
        finalizeTriggerInstanceWithCancelling(all.get(i));
      }
      //sleep for a while to ensure endtime is different for each trigger instance
      Thread.sleep(100);
    }

    this.shuffleAndUpload(all);

    final List<TriggerInstance> finished = all.subList(7, all.size());

    final List<TriggerInstance> expected = new ArrayList<>(finished);
    expected.sort(Comparator.comparing(TriggerInstance::getStartTime));

    final Collection<TriggerInstance> running = this.triggerInstLoader.getRunning();
    assertTwoTriggerInstanceListsEqual(new ArrayList<>(running), expected, true, true);
  }

  private void assertTwoTriggerInstanceListsEqual(final List<TriggerInstance> actual,
      final List<TriggerInstance> expected, final boolean ignoreFlowTrigger,
      final boolean keepOriginalOrder) {
    if (!keepOriginalOrder) {
      expected.sort(Comparator.comparing(TriggerInstance::getId));
      actual.sort(Comparator.comparing(TriggerInstance::getId));
    }

    assertThat(actual).hasSameSizeAs(expected);
    final Iterator<TriggerInstance> it1 = actual.iterator();
    final Iterator<TriggerInstance> it2 = expected.iterator();
    while (it1.hasNext() && it2.hasNext()) {
      //8bfafb89-ac79-45a0-a049-b55038b0886b
      assertTriggerInstancesEqual(it1.next(), it2.next(), ignoreFlowTrigger);
    }
  }

  @Test
  public void testUpdateAssociatedFlowExecId() {
    final TriggerInstance expectedTriggerInst = this.createTriggerInstance(this.flowTrigger, this
        .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis());
    this.triggerInstLoader.uploadTriggerInstance(expectedTriggerInst);
    finalizeTriggerInstanceWithSuccess(expectedTriggerInst, 1000);

    expectedTriggerInst.getDepInstances()
        .forEach(depInst -> this.triggerInstLoader.updateDependencyExecutionStatus(depInst));

    this.triggerInstLoader.updateAssociatedFlowExecId(expectedTriggerInst);

    final TriggerInstance actualTriggerInst = this.triggerInstLoader
        .getTriggerInstanceById(expectedTriggerInst.getId());

    assertTriggerInstancesEqual(actualTriggerInst, expectedTriggerInst, false);
  }

  @Test
  public void testGetRecentlyFinishedReturnsEmpty() {
    final List<TriggerInstance> all = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()));
    }

    this.shuffleAndUpload(all);

    final Collection<TriggerInstance> recentlyFinished = this.triggerInstLoader
        .getRecentlyFinished(10);
    assertThat(recentlyFinished).isEmpty();
  }

  @Test
  public void testGetTriggerInstancesStartTimeDesc() {
    final List<TriggerInstance> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expected.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()
          + i * 1000));
    }

    this.shuffleAndUpload(expected);
    final Collection<TriggerInstance> actual = this.triggerInstLoader.getTriggerInstances
        (this.project_id, this.flow_id, 0, 10);
    expected.sort((o1, o2) -> ((Long) o2.getStartTime()).compareTo(o1.getStartTime()));

    assertTwoTriggerInstanceListsEqual(new ArrayList<>(actual), new ArrayList<>(expected), true,
        true);
  }

  @Test
  public void testGetEmptyTriggerInstancesStartTimeDesc() {
    final Collection<TriggerInstance> actual = this.triggerInstLoader.getTriggerInstances
        (this.project_id, this.flow_id, 0, 10);
    assertThat(actual).isEmpty();
  }

  @Test
  public void testDeleteOldTriggerInstances() throws InterruptedException {
    final List<TriggerInstance> all = new ArrayList<>();
    final long ts1 = System.currentTimeMillis();
    long ts2 = -1;
    long ts3 = -1;
    for (int i = 0; i < 30; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()
          + i * 10000));

      if (i < 5) {
        finalizeTriggerInstanceWithSuccess(all.get(i), -1);
      } else if (i <= 15) {
        finalizeTriggerInstanceWithCancelled(all.get(i));
      } else if (i <= 25) {
        finalizeTriggerInstanceWithCancelling(all.get(i));
      } else if (i <= 27) {
        finalizeTriggerInstanceWithSuccess(all.get(i), 1000);
      }
      //sleep for a while to ensure end time is different for each trigger instance
      if (i == 3) {
        ts2 = System.currentTimeMillis();
      } else if (i == 12) {
        ts3 = System.currentTimeMillis();
      }
      Thread.sleep(100);
    }
    this.shuffleAndUpload(all);

    assertThat(this.triggerInstLoader.deleteCompleteTriggerExecutionFinishingOlderThan(ts1))
        .isEqualTo(0);

    assertThat(this.triggerInstLoader.deleteCompleteTriggerExecutionFinishingOlderThan(ts2))
        .isEqualTo(0);

    assertThat(this.triggerInstLoader.deleteCompleteTriggerExecutionFinishingOlderThan(ts3))
        .isEqualTo(16);

  }

  @Test
  public void testGetRecentlyFinished() throws InterruptedException {

    final List<TriggerInstance> all = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      all.add(this.createTriggerInstance(this.flowTrigger, this
          .flow_id, this.flow_version, this.submitUser, this.project, System.currentTimeMillis()
          + i * 10000));
      if (i <= 3) {
        finalizeTriggerInstanceWithCancelled(all.get(i));
      } else if (i <= 6) {
        finalizeTriggerInstanceWithSuccess(all.get(i), 1000);
      } else if (i <= 9) {
        finalizeTriggerInstanceWithCancelling(all.get(i));
      }
      //sleep for a while to ensure endtime is different for each trigger instance
      Thread.sleep(100);
    }

    this.shuffleAndUpload(all);

    final List<TriggerInstance> finished = all.subList(0, 7);
    finished.sort((o1, o2) -> ((Long) o2.getEndTime()).compareTo(o1.getEndTime()));

    List<TriggerInstance> expected = new ArrayList<>(finished);
    expected.sort(Comparator.comparing(TriggerInstance::getStartTime));

    Collection<TriggerInstance> recentlyFinished = this.triggerInstLoader
        .getRecentlyFinished(10);
    assertTwoTriggerInstanceListsEqual(new ArrayList<>(recentlyFinished), expected, true, true);

    expected = new ArrayList<>(finished.subList(0, 3));
    expected.sort(Comparator.comparing(TriggerInstance::getStartTime));
    recentlyFinished = this.triggerInstLoader.getRecentlyFinished(3);
    assertTwoTriggerInstanceListsEqual(new ArrayList<>(recentlyFinished), expected, true, true);

    expected = new ArrayList<>(finished.subList(0, 1));
    recentlyFinished = this.triggerInstLoader.getRecentlyFinished(1);
    assertTwoTriggerInstanceListsEqual(new ArrayList<>(recentlyFinished), expected, true, true);
  }

  @After
  public void cleanDB() {
    try {
      dbOperator.update("TRUNCATE TABLE execution_dependencies");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }
}
