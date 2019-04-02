package azkaban.user;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.apache.log4j.Logger;

public final class UserUtils {

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
  public static void setupWatch(final String fileName, Logger log, ParseConfigFile parser) {
    Runnable runnable = () -> {
      WatchService watchService;
      Path path;
      try {
        watchService = FileSystems.getDefault().newWatchService();
        final File file = new File(fileName);
        final String dirPath = file.getParent();
        path = Paths.get(dirPath);
        path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_CREATE);
      } catch (IOException e) {
        // Ignore the IOException
        log.warn("IOException while setting up watch on conf"
            + e.getMessage());
        return;
      }
      for (;;) {
        // Watch for modifications
        WatchKey watchKey;
        try {
          watchKey = watchService.take();
        } catch (InterruptedException ie) {
          log.warn(ie.getMessage());
          return;
        }

        for (WatchEvent<?> event : watchKey.pollEvents()) {
          // Make sure the modification happened to user xml
          @SuppressWarnings("unchecked")
          final Path name = ((WatchEvent<Path>)event).context();
          final Path child = path.resolve(name);
          if (!child.toString().equals(fileName)) {
            continue; // not user xml
          }
          // reparse the XML
          log.info("Modification detected, reloading user config");
          parser.parseConfigFile();
        }
        watchKey.reset();
      }
    };

    final Thread thread = new Thread(runnable);
    System.out.println("Starting thread");
    thread.start();
  }
}
