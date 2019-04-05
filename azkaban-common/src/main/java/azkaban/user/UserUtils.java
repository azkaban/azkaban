package azkaban.user;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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
import java.util.HashMap;
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
    Preconditions.checkNotNull(configFileMap);
    Preconditions.checkArgument(configFileMap.size() > 0);

    Runnable runnable = () -> {
      WatchService watchService  = null;
      // Map to store WatchKey to Dir mapping
      final Map<WatchKey, Path> keys = new HashMap<>();
      // A directory to config files multimap
      final Multimap<Path, String> dirToFilesMap = HashMultimap.create();

      // Iterate over each file.
      for (Map.Entry<String, ParseConfigFile> entry : configFileMap.entrySet()) {
        String fileName = entry.getKey();
        ParseConfigFile parser = entry.getValue();
        Preconditions.checkNotNull(fileName);
        Preconditions.checkNotNull(parser);

        final File file = new File(fileName);
        if (!file.exists()) {
          log.warn("Failed to reload config, user provided file " + fileName + " does not exist.");
          continue;
        }

        try {
          watchService = FileSystems.getDefault().newWatchService();
          Path dir = Paths.get(fileName).getParent();
          if (!dirToFilesMap.containsKey(dir)) {
            // There is not entry for this directory, create a watchkey
            WatchKey watchKey = dir.register(watchService,
                new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY},
                SensitivityWatchEventModifier.HIGH);
            keys.put(watchKey, dir);
          }
          // Add the config file to dir map
          dirToFilesMap.put(dir, fileName);
        } catch (IOException e) {
          // Ignore the IOException
          log.warn("IOException while setting up watch on conf " + fileName + ". "
              + e.getMessage());
          Thread.currentThread().interrupt();
          return;
        }
      }

      // Return if WatchService is not initialized
      if (watchService == null) {
        log.warn("Watchservice not setup for any config file(s).");
        Thread.currentThread().interrupt();
        return;
      }

      // Watchservice is established, now listen for the events till eternity!
      for (;; ) {
        WatchKey watchKey;
        try {
          watchKey = watchService.take();
        } catch (InterruptedException ie) {
          log.warn(ie.getMessage());
          Thread.currentThread().interrupt();
          return;
        }

        // Get the directory for which watch service event triggered.
        Path dir = keys.get(watchKey);
        for (WatchEvent<?> event : watchKey.pollEvents()) {
          // Make sure the modification happened to user config file
          @SuppressWarnings("unchecked")
          final Path name = ((WatchEvent<Path>) event).context();
          final String child = dir.resolve(name).toString();
          // Lookup the file in dirToFilesMap
          for (String fileName : dirToFilesMap.get(dir)) {
            if (!child.equals(fileName)) {
              continue;
            }

            // Match!
            // reparse the config file
            log.info("Modification detected, reloading user config");
            configFileMap.get(fileName).parseConfigFile();
            break;
          }
        }
        watchKey.reset();
      }
    };

    final Thread thread = new Thread(runnable);
    log.info("Starting configuration watching thread.");
    thread.start();
  }
}
