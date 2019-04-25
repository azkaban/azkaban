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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UserUtils {

  private static final Logger log = LoggerFactory.getLogger(UserUtils.class);

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
  static void setupWatch(final Map<String, ParseConfigFile> configFileMap) throws IOException {
    Preconditions.checkNotNull(configFileMap);
    Preconditions.checkArgument(configFileMap.size() > 0);

    final WatchService watchService;
    try {
      watchService = FileSystems.getDefault().newWatchService();
    } catch (final IOException e) {
      log.warn(" Failed to create WatchService " + e.getMessage());
      throw e;
    }

    // Map to store WatchKey to Dir mapping
    final Map<WatchKey, Path> keys = new HashMap<>();
    // A directory to config files multimap
    final Multimap<Path, String> dirToFilesMap = HashMultimap.create();

    // Iterate over each file.
    for (final Map.Entry<String, ParseConfigFile> entry : configFileMap.entrySet()) {
      final String fileName = entry.getKey();
      final ParseConfigFile parser = entry.getValue();
      Preconditions.checkNotNull(fileName);
      Preconditions.checkNotNull(parser);

      final File file = new File(fileName);
      if (!file.exists()) {
        log.warn("Failed to setup watch service, user provided file " + fileName + " does not "
            + "exist.");
        continue;
      }

      try {
        final Path dir = Paths.get(fileName).getParent();
        if (!dirToFilesMap.containsKey(dir)) {
          // There is no entry for this directory, create a watchkey
          final WatchKey watchKey = dir.register(watchService,
              new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY},
              SensitivityWatchEventModifier.MEDIUM);
          keys.put(watchKey, dir);
        }
        // Add the config file to dir map
        dirToFilesMap.put(dir, fileName);
      } catch (final IOException e) {
        // Ignore the IOException
        log.warn("IOException while setting up watch on conf " + fileName + ". "
            + e.getMessage());
      }
    }

    // Return if WatchService is not initialized
    if (keys.size() == 0) {
      log.warn("Watchservice was not setup for any config file(s).");
      try {
        watchService.close();
      } catch (final IOException e) {
        log.warn("IOException while closing watchService. " + e.getMessage());
      }
      return;
    }

    final Runnable runnable = () -> {
      // Watchservice is established, now listen for the events till eternity!
      for (; ; ) {
        final WatchKey watchKey;
        try {
          watchKey = watchService.take();
          // Wait for a second to ensure there is only one event for a modification.
          // WatchService otherwise creates two events 1 for content and 1 for modification time.
          Thread.sleep(1000L);
        } catch (final InterruptedException ie) {
          log.warn(ie.getMessage());
          Thread.currentThread().interrupt();
          return;
        }

        // Get the directory for which watch service event triggered.
        final Path dir = keys.get(watchKey);
        for (final WatchEvent<?> event : watchKey.pollEvents()) {
          // Make sure the modification happened to user config file
          @SuppressWarnings("unchecked") final Path name = ((WatchEvent<Path>) event).context();
          final String filename = dir.resolve(name).toString();
          // Lookup the file in dirToFilesMap
          if (dirToFilesMap.containsEntry(dir, filename)) {
            // Match!
            // reparse the config file
            log.info("Modification detected, reloading config file " + filename + ".");
            configFileMap.get(filename).parseConfigFile();
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
