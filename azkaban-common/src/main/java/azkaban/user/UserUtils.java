package azkaban.user;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
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
  static void setupWatch(final Map<String, ParseConfigFile> configFileMap,
      final FileWatcher watcher) {
    Preconditions.checkNotNull(configFileMap);
    Preconditions.checkArgument(configFileMap.size() > 0);

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
          final WatchKey watchKey = watcher.register(dir);
          keys.put(watchKey, dir);
        }
        // Add the config file to dir map
        dirToFilesMap.put(dir, fileName);
      } catch (final IOException e) {
        // Ignore the IOException
        log.warn("IOException while setting up watch on conf " + fileName + ". ", e);
      }
    }

    // Return if WatchService is not initialized
    if (keys.size() == 0) {
      log.warn("Watchservice was not setup for any config file(s).");
      try {
        watcher.close();
      } catch (final IOException e) {
        log.warn("IOException while closing watchService. ", e);
      }
      return;
    }

    final Runnable runnable = () -> {
      // Watchservice is established, now listen for the events till eternity!
      for (; ; ) {
        final WatchKey watchKey;
        try {
          watchKey = watcher.take();
        } catch (final InterruptedException ie) {
          log.warn(ie.toString());
          Thread.currentThread().interrupt();
          return;
        }

        // Get the directory for which watch service event triggered.
        final Path dir = keys.get(watchKey);
        for (final WatchEvent<?> event : watcher.pollEvents(watchKey)) {
          // Make sure the modification happened to user config file
          @SuppressWarnings("unchecked") final Path name = ((WatchEvent<Path>) event).context();
          final String filename = dir.resolve(name).toString();
          // Lookup the file in dirToFilesMap
          if (!dirToFilesMap.containsEntry(dir, filename)) {
            continue;
          }
          // Match!
          // Reparse the config file
          log.info("Modification detected, reloading config file " + filename + ".");
          try {
            configFileMap.get(filename).parseConfigFile();
          } catch (final Exception e) {
            // If there is any exception while parsing the config file, log it and move on
            log.warn("Reload failed for config file " + filename + " due to ", e);
          }
        }
      }
    };

    final Thread thread = new Thread(runnable);
    // allow JVM to terminate without waiting for this thread if the app is shutting down
    thread.setDaemon(true);
    log.info("Starting configuration watching thread.");
    thread.start();
  }
}
