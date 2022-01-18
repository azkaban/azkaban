package azkaban.utils;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used as utility class for ExecuteAsUser script.
 */
public class ExecuteAsUserUtils {

  private static final Logger logger = LoggerFactory.getLogger(ExecuteAsUserUtils.class);
  private static final String ADD_GROUP_COMMAND = "/usr/sbin/groupadd";
  private static final String ADD_USER_COMMAND = "/usr/sbin/useradd";

  /**
   * Add group and user on environment where job will run before job process starts.
   *
   * @param effectiveUser
   * @throws Exception
   */
  public static void addGroupAndUserForEffectiveUser(final ExecuteAsUser executeAsUser,
      final String effectiveUser) throws Exception {
    List<String> commands = Arrays.asList(ADD_GROUP_COMMAND, effectiveUser);
    int result = executeAsUser.execute("root", commands);
    if (result != 0) {
      final String errorMessage = "Failed to add group: " + result;
      //Log the error message and don't throw exception as this method is called for every job.
      //If all the jobs have same effective user then first time, adding group and user will pass
      //but for second time, it will fail. So fail silently.
      logger.error(errorMessage);
    }
    commands =Arrays
        .asList(ADD_USER_COMMAND, "-l", "-m", effectiveUser, "-g", effectiveUser);
    result = executeAsUser.execute("root", commands);
    if (result != 0) {
      final String errorMessage = "Failed to add user: " + result;
      logger.error(errorMessage);
    }
  }
}
