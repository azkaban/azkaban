package azkaban.imagemgmt.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageTypeDaoImpl implements ImageTypeDao {
  private static final Logger log = LoggerFactory.getLogger(ImageTypeDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  static String INSERT_IMAGE_TYPE =
      "insert into image_types ( type, description, active, deployable, created_on, created_by ) "
          + "values (?, ?, ?, ?, ?, ?)";
  static String INSERT_IMAGE_OWNERSHIP =
      "insert into image_ownerships ( type_id, owner, role, created_on, created_by ) "
          + "values (?, ?, ?, ?, ?)";

  @Inject
  public ImageTypeDaoImpl(DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  @Override
  public int createImageType(ImageType imageType) {
    final SQLTransaction<Integer> insertAndGetSpaceId = transOperator -> {
      String currentTime = DateTime.now().toLocalDateTime().toString();
      transOperator.update(INSERT_IMAGE_TYPE, imageType.getType(), imageType.getDescription(),
          true, imageType.getDeployable().getName(), currentTime, imageType.getCreatedBy());
      int imageTypeId = Long.valueOf(transOperator.getLastInsertId()).intValue();
      if( imageType.getOwnerships() != null && imageType.getOwnerships().size() > 0) {
        for(ImageOwnership imageOwnership : imageType.getOwnerships()) {
          transOperator.update(INSERT_IMAGE_OWNERSHIP, imageTypeId, imageOwnership.getOwner(),
              imageOwnership.getRole(), currentTime, imageType.getCreatedBy());
        }
      }
      transOperator.getConnection().commit();
      return imageTypeId;
    };

    int imageTypeId = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      imageTypeId = databaseOperator.transaction(insertAndGetSpaceId);
      log.info("Created image type id :"+imageTypeId);
    } catch (SQLException e) {
      log.error("Unable to create the image type metadata", e);
    }
    return imageTypeId;
  }

  @Override
  public Optional<ImageType> getImageTypeByType(String type) {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    List<ImageType> imageTypes = new ArrayList<>();
    try {
      imageTypes = this.databaseOperator
          .query(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_TYPE, fetchImageTypeHandler, type);
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_TYPE + " failed.", ex);
      throw new RuntimeException("Unable to fetch image type metadata from image type : "+type);
    }
    return imageTypes.isEmpty() ? Optional.empty() : Optional.of(imageTypes.get(0));
  }

  public static class FetchImageTypeHandler implements ResultSetHandler<List<ImageType>> {
    private static final String FETCH_IMAGE_TYPE_BY_ID =
        "SELECT id, type, description, active, deployable, created_on, created_by FROM image_types"
            + " WHERE id = ?";
    private static final String FETCH_IMAGE_TYPE_BY_TYPE =
        "SELECT id, type, description, active, deployable, created_on, created_by FROM image_types"
            + " WHERE type = ?";
    private static final String FETCH_ALL_IMAGE_TYPES =
        "SELECT id, type, description, active, deployable, created_on, created_by FROM image_types";
    @Override
    public List<ImageType> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageType> imageTypes = new ArrayList<>();
      do {
        final int id = rs.getInt("id");
        final String type = rs.getString("type");
        final String description = rs.getString("description");
        final boolean active = rs.getBoolean("active");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final ImageType imageType = new ImageType();
        imageType.setId(id);
        imageType.setType(type);
        imageType.setDescription(description);
        imageType.setCreatedOn(createdOn);
        imageType.setCreatedBy(createdBy);
        imageTypes.add(imageType);
      } while (rs.next());
      return imageTypes;
    }
  }
}
