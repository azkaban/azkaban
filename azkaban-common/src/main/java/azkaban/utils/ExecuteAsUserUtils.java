package azkaban.utils;

import azkaban.jobExecutor.utils.JobExecutionException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used as utility class for ExecuteAsUser script.
 */
public class ExecuteAsUserUtils {

  private static final Logger logger = LoggerFactory.getLogger(ExecuteAsUserUtils.class);
  private static final String ADD_GROUP_PATH = "/usr/sbin/groupadd";
  private static final String ADD_USER_PATH = "/usr/sbin/useradd";

  /**
   * Add group and user on environment where job will run before job process starts. These commands
   * can only be executed by root user.
   *
   * @param effectiveUser
   * @throws Exception
   */
  public static void addGroupAndUserForEffectiveUser(final ExecuteAsUser executeAsUser,
      final String effectiveUser) throws Exception {
    List<String> commands = Arrays.asList(ADD_GROUP_PATH, effectiveUser);
    int result = executeAsUser.execute("root", commands);
    // If group is added successfully, it will return code 0.
    // If group is already added then it will return code 9.
    if (result == 0 || result == 9) {
      logger.info("Group is added successfully.");
      commands = Arrays
          .asList(ADD_USER_PATH, "-l", "-m", effectiveUser, "-g", effectiveUser);
      result = executeAsUser.execute("root", commands);
      // If user is added successfully, it will return code 0.
      // If user is already added then it will return code 9.
      if (result == 0 || result == 9) {
        logger.info("User is added successfully.");
      } else {
        final String errorMessage =
            "Failed to add user:" + effectiveUser + "Return value was: " + result;
        logger.error(errorMessage);
        throw new JobExecutionException(errorMessage);
      }
    } else {
      final String errorMessage = "Failed to add group:" + effectiveUser + "Return value was: " + result;
      logger.error(errorMessage);
      throw new JobExecutionException(errorMessage);
    }
  }
}
