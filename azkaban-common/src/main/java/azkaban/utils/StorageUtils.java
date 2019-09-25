package azkaban.utils;

import azkaban.spi.Dependency;
import org.apache.commons.codec.binary.Hex;

import static azkaban.utils.ThinArchiveUtils.convertIvyCoordinateToPath;


public class StorageUtils {
  public static String getTargetProjectFilename(final int projectId, byte[] hash) {
    return String.format("%s-%s.zip",
        String.valueOf(projectId),
        new String(Hex.encodeHex(hash))
    );
  }

  public static String getTargetDependencyPath(Dependency dep) {
    // For simplicity, we will set the path to store dependencies the same as the in the URL used
    // for fetching the dependencies. It will follow the pattern:
    // samsa/samsa-api/0.6.0/samsa-api-0.6.0.jar
    return convertIvyCoordinateToPath(dep);
  }
}
