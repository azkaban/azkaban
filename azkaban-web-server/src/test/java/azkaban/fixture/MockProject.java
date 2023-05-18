package azkaban.fixture;

import azkaban.project.FeatureFlag;
import azkaban.project.Project;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Provide a mock project.
 */
public class MockProject {

  /**
   * Get mock project project.
   *
   * @return a mock project for testing
   */
  public static Project getMockProject() {
    final Project project = new Project(1, "test_project");
    project.setDescription("My project description");
    project.setLastModifiedUser("last_modified_user_name");
    project.setUploadLock(true);
    project.addFeatureFlags(FeatureFlag.ENABLE_PROJECT_ADHOC_UPLOAD, false);

    final DateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
    try {
      final Date modifiedDate = dateFormat.parse("2000:01:02:3:4:5");

      final long modifiedTime = modifiedDate.getTime();
      project.setLastModifiedTimestamp(modifiedTime);

      final Date createDate = dateFormat.parse("1999:06:07:8:9:10");

      final long createTime = createDate.getTime();

      project.setCreateTimestamp(createTime);
    } catch (final ParseException e) {
      e.printStackTrace();
    }
    return project;
  }
}
