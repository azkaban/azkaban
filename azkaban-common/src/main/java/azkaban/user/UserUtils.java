package azkaban.user;

import com.google.common.base.Preconditions;
import com.sun.nio.file.SensitivityWatchEventModifier;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import org.apache.log4j.Logger;

public final class UserUtils {

  private static final Logger log = Logger.getLogger(UserUtils.class);
  private UserUtils() {

  }

  /**
   * @return - Returns true if the given user is an ADMIN, or if user has the required permission
   * for the action requested.
   */
  public static boolean hasPermissionforAction(final UserManager userManager, final User user,
      final Permission.Type type) {
    for (final String roleName : user.getRoles()) {
      final Role role = userManager.getRole(roleName);
      final Permission perm = role.getPermission();
      if (perm.isPermissionSet(Permission.Type.ADMIN) || perm.isPermissionSet(type)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Creates a watch thread which listens to specified files' modification and reloads
   * configurations
   */
  static void setupWatch(final Map<String, ParseConfigFile> configFileMap) {
    Preconditions.checkArgument(configFileMap != null);

    Runnable runnable = () -> {
      WatchService watchService;
      Path path;
      for (Map.Entry<String, ParseConfigFile> entry : configFileMap.entrySet()) {
        String fileName = entry.getKey();
        ParseConfigFile parser = entry.getValue();
        Preconditions.checkArgument(fileName != null && parser != null);

        final File file = new File(fileName);
        if (!file.exists()) {
          throw new IllegalArgumentException("User provided file " + fileName + " does not exist.");
        }

        try {
          watchService = FileSystems.getDefault().newWatchService();
          path = Paths.get(fileName).getParent();
          path.register(watchService, new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY},
              SensitivityWatchEventModifier.HIGH);
        } catch (IOException e) {
          // Ignore the IOException
          log.warn("IOException while setting up watch on conf"
              + e.getMessage());
          return;
        }
        for (; ; ) {
          // Watch for modifications
          WatchKey watchKey;
          try {
            watchKey = watchService.take();
          } catch (InterruptedException ie) {
            log.warn(ie.getMessage());
            return;
          }

          for (WatchEvent<?> event : watchKey.pollEvents()) {
            // Make sure the modification happened to user config file
            @SuppressWarnings("unchecked")
            final Path name = ((WatchEvent<Path>) event).context();
            final Path child = path.resolve(name);
            if (!child.toString().equals(fileName)) {
              continue; // not user xml
            }
            // reparse the config file
            log.info("Modification detected, reloading user config");
            parser.parseConfigFile();
          }
          watchKey.reset();
        }
      }
    };

    final Thread thread = new Thread(runnable);
    log.info("Starting configuration watching thread.");
    thread.start();
  }
}
