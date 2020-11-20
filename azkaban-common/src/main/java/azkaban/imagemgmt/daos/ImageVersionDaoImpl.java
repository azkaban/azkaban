/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.imagemgmt.daos;

import azkaban.Constants.ImageMgmtConstants;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.exeception.ImageMgmtDaoException;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.dto.RequestContext;
import com.google.common.collect.Iterables;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAO Implementation for accessing image version metadata. This class contains implementation for
 * methods such as create, get image version metadata etc.
 */
@Singleton
public class ImageVersionDaoImpl implements ImageVersionDao {

  private static final Logger log = LoggerFactory.getLogger(ImageVersionDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  private final ImageTypeDao imageTypeDao;

  private static String INSERT_IMAGE_VERSION_QUERY =
      "insert into image_versions ( path, description, version, type_id, state, "
          + "release_tag, created_by, modified_by) "
          + "values (?, ?, ?, ?, ?, ?, ?, ?)";
  private static String SELECT_IMAGE_VERSION_BASE_QUERY = "select iv.id, iv.path, iv.description, "
      + "iv.version, it.name, iv.state, iv.release_tag, iv.created_on, iv.created_by, iv"
      + ".modified_on, iv.modified_by from image_versions iv, image_types it where it.id = iv.type_id";

  @Inject
  public ImageVersionDaoImpl(DatabaseOperator databaseOperator, ImageTypeDao imageTypeDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
  }

  @Override
  public int createImageVersion(ImageVersion imageVersion) {
    ImageType imageType = imageTypeDao.getImageTypeByName(imageVersion.getName())
        .orElseThrow(() -> new ImageMgmtDaoException("Unable to fetch image type metadata. Invalid "
            + "image type : "+imageVersion.getName()));
    final SQLTransaction<Long> insertAndGetSpaceId = transOperator -> {
      transOperator.update(INSERT_IMAGE_VERSION_QUERY, imageVersion.getPath(), imageVersion.getDescription(),
          imageVersion.getVersion(), imageType.getId(), imageVersion.getState().getStateValue(),
          imageVersion.getReleaseTag(), imageVersion.getCreatedBy(), imageVersion.getCreatedBy());
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
      throw  new ImageMgmtDaoException("Exception while creating image version metadata");
    }
    return imageVersionId;
  }

  @Override
  public List<ImageVersion> findImageVersions(RequestContext requestContext) throws ImageMgmtException {
    List<ImageVersion> imageVersions = new ArrayList<>();
    try {
      StringBuilder queryBuilder = new StringBuilder(SELECT_IMAGE_VERSION_BASE_QUERY);
      List<Object> params = new ArrayList<>();
      // add imageType in the query
      if(requestContext.getParams().containsKey(ImageMgmtConstants.IMAGE_TYPE)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" it.name = ?");
        params.add(requestContext.getParams().get(ImageMgmtConstants.IMAGE_TYPE));
      }
      // add imageVersion in the query if present
      if(requestContext.getParams().containsKey(ImageMgmtConstants.IMAGE_VERSION)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" iv.version = ?");
        params.add(requestContext.getParams().get(ImageMgmtConstants.IMAGE_VERSION));
      }
      // add versionState in the query if present
      if(requestContext.getParams().containsKey(ImageMgmtConstants.VERSION_STATE)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" iv.state = ?");
        State versionState = (State)requestContext.getParams().get(ImageMgmtConstants.VERSION_STATE);
        params.add(versionState.getStateValue());
      }
      log.info("Image version get query : "+queryBuilder.toString());
      imageVersions = databaseOperator.query(queryBuilder.toString(),
          new FetchImageVersionHandler(), Iterables.toArray(params, Object.class));
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw  new ImageMgmtDaoException("Exception while fetching image version");
    }
    return imageVersions;
  }

  /**
   * ResultSetHandler implementation class for fetching image version
   */
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
        final String name = rs.getString("name");
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
        imageVersion.setName(name);
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
