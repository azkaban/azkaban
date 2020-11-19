package azkaban.imagemgmt.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.utils.ImageMgmtConstants;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ImageVersionDaoImpl implements ImageVersionDao {

  private static final Logger log = LoggerFactory.getLogger(ImageVersionDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  private final ImageTypeDao imageTypeDao;

  private static String INSERT_IMAGE_VERSION_QUERY =
      "insert into image_versions ( path, description, version, type_id, state, "
          + "release_tag, created_on, created_by) "
          + "values (?, ?, ?, ?, ?, ?, ?, ?)";
  private static String SELECT_IMAGE_VERSION_BASE_QUERY = "select iv.id, iv.path, iv.description, "
      + "iv.version, it.type, iv.state, iv.release_tag, iv.created_on, iv.created_by, iv.modified_on, "
      + "iv.modified_by from image_versions iv, image_types it where it.id = iv.type_id";

  @Inject
  public ImageVersionDaoImpl(DatabaseOperator databaseOperator, ImageTypeDao imageTypeDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
  }

  @Override
  public int createImageVersion(ImageVersion imageVersion) {
    ImageType imageType = imageTypeDao.getImageTypeByType(imageVersion.getType())
        .orElseThrow(() -> new RuntimeException("Unable to fetch image type metadata. Invalid "
            + "image type : "+imageVersion.getType()));
    final SQLTransaction<Long> insertAndGetSpaceId = transOperator -> {
      String currentTime = DateTime.now().toLocalDateTime().toString();
      transOperator.update(INSERT_IMAGE_VERSION_QUERY, imageVersion.getPath(), imageVersion.getDescription(),
          imageVersion.getVersion(), imageType.getId(), imageVersion.getState().getStateValue(),
          imageVersion.getReleaseTag(), currentTime, imageVersion.getCreatedBy());
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    int imageVersionId = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      imageVersionId = databaseOperator.transaction(insertAndGetSpaceId).intValue();
    } catch (SQLException e) {
      log.error("Unable to create the image version metadata", e);
    }
    return imageVersionId;
  }

  @Override
  public List<ImageVersion> getImageVersion(RequestContext requestContext) throws ImageMgmtException {
    List<ImageVersion> imageVersions = new ArrayList<>();
    try {
      StringBuilder queryBuilder = new StringBuilder(SELECT_IMAGE_VERSION_BASE_QUERY);
      if(requestContext.getParams().containsKey(ImageMgmtConstants.IMAGE_TYPE)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" it.type = '"+requestContext.getParams().get(ImageMgmtConstants.IMAGE_TYPE)+"'");
      }

      if(requestContext.getParams().containsKey(ImageMgmtConstants.IMAGE_VERSION)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" iv.version = '"+requestContext.getParams().get(ImageMgmtConstants.IMAGE_VERSION)+"'");
      }
      log.info("Query : "+queryBuilder.toString());
      imageVersions = databaseOperator.query(queryBuilder.toString(),
          new FetchImageVersionHandler());
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw  new ImageMgmtException("Exception while fetching image version");
    }
    return imageVersions;
  }

  public static class FetchImageVersionHandler implements ResultSetHandler<List<ImageVersion>> {
    @Override
    public List<ImageVersion> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageVersion> imageVersions = new ArrayList<>();
      do {
        final int id = rs.getInt("id");
        final String path = rs.getString("path");
        final String description = rs.getString("description");
        final String version = rs.getString("version");
        final String type = rs.getString("type");
        final String state = rs.getString("state");
        final String releaseTag = rs.getString("release_tag");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageVersion imageVersion = new ImageVersion();
        imageVersion.setId(id);
        imageVersion.setPath(path);
        imageVersion.setDescription(description);
        imageVersion.setVersion(version);
        imageVersion.setState(State.fromStateValue(state));
        imageVersion.setReleaseTag(releaseTag);
        imageVersion.setType(type);
        imageVersion.setCreatedOn(createdOn);
        imageVersion.setCreatedBy(createdBy);
        imageVersion.setModifiedBy(modifiedBy);
        imageVersion.setModifiedOn(modifiedOn);
        imageVersions.add(imageVersion);
      } while (rs.next());
      return imageVersions;
    }
  }
}
