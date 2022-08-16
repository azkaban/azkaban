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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;
import static azkaban.Constants.ImageMgmtConstants.IMAGE_VERSION;
import static azkaban.Constants.ImageMgmtConstants.VERSION_STATE;
import static azkaban.imagemgmt.utils.ErroCodeConstants.SQL_ERROR_CODE_DATA_TOO_LONG;
import static azkaban.imagemgmt.utils.ErroCodeConstants.SQL_ERROR_CODE_DUPLICATE_ENTRY;

import azkaban.Constants.ImageMgmtConstants;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import com.google.common.collect.Iterables;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.text.StrSubstitutor;
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

  private static final String INSERT_IMAGE_VERSION_QUERY =
      "insert into image_versions ( path, description, version, type_id, state, release_tag, "
          + "created_by, created_on, modified_by, modified_on) "
          + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String SELECT_IMAGE_VERSION_BASE_QUERY =
      "select iv.id, iv.path, iv.description, "
          + "iv.version, cast(replace(iv.version, '.', '') as unsigned integer) as int_version, "
          + "it.name, iv.state, iv.release_tag, iv.created_on, iv.created_by, iv.modified_on, "
          + "iv.modified_by from image_versions iv, image_types it where it.id = iv.type_id";

  /*
   * The below query uses calculated column to get the image version in integer format. The query
   * consists for two inner queries. One inner query selects the max image version using the
   * calculated int_version field for the image types. The second inner query matches the max
   * version computed above and selects the corresponding image version records. NOTE: The computed
   * column can't be used directly in the where clause hence, two inner tables are created on top of
   * the inner queries.
   */
  private static final String SELECT_LATEST_IMAGE_VERSION_QUERY = "select outer_tbl.id, "
      + "outer_tbl.path, outer_tbl.description, outer_tbl.version, outer_tbl.name, outer_tbl.state, "
      + "outer_tbl.release_tag, outer_tbl.created_on, outer_tbl.created_by, outer_tbl.modified_on, "
      + "outer_tbl.modified_by from (select iv.id, iv.path, iv.description, iv.version, "
      + "cast(replace(iv.version, '.', '') as unsigned integer) as int_version, it.name, iv.state, "
      + "iv.release_tag, iv.created_on, iv.created_by, iv.modified_on, iv.modified_by "
      + "from image_versions iv, image_types it where it.id = iv.type_id and iv.state in "
      + "( ${version_states} )  and lower(it.name) in ( ${image_types} )) "
      + "outer_tbl where outer_tbl.int_version in (select max(inner_tbl.int_version) max_version "
      + "from (select it.name, cast(replace(iv.version, '.', '') as unsigned integer) as int_version "
      + "from image_versions iv, image_types it where it.id = iv.type_id and iv.state in "
      + "( ${version_states} )  and lower(it.name) in ( ${image_types} )) "
      + "inner_tbl group by inner_tbl.name);";

  private static final String SELECT_IMAGE_VERSION_BY_TYPE_AND_VERSION_ID =
      "select iv.id, iv.path, "
          + "iv.description, iv.version, iv.version, it.name, iv.state, iv.release_tag, iv.created_on, "
          + "iv.created_by, iv.modified_on, iv.modified_by from image_versions iv, image_types it where "
          + "it.id = iv.type_id and lower(it.name) = ? and iv.id = ?";

  private static final String SELECT_INVALID_IMAGE_VERSION_QUERY = "select not exists (select "
      + "iv.version from image_versions iv inner join image_types it on it.id = iv.type_id "
      + "where lower(it.name) = ? and iv.version = ? and iv.state in (?, ?)) is_invalid";

  @Inject
  public ImageVersionDaoImpl(final DatabaseOperator databaseOperator,
      final ImageTypeDao imageTypeDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
  }

  @Override
  public int createImageVersion(final ImageVersion imageVersion) {
    final ImageType imageType = this.imageTypeDao.getImageTypeByName(imageVersion.getName())
        .orElseThrow(() -> new ImageMgmtDaoException("Unable to fetch image type metadata. Invalid "
            + "image type : " + imageVersion.getName()));
    final SQLTransaction<Long> insertAndGetId = transOperator -> {
      // Passing timestamp from the code base and can be formatted accordingly based on timezone
      final Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
      transOperator
          .update(INSERT_IMAGE_VERSION_QUERY, imageVersion.getPath(), imageVersion.getDescription(),
              imageVersion.getVersion(), imageType.getId(), imageVersion.getState().getStateValue(),
              imageVersion.getReleaseTag(), imageVersion.getCreatedBy(), currentTimestamp,
              imageVersion.getModifiedBy(), currentTimestamp);
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    int imageVersionId = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      imageVersionId = this.databaseOperator.transaction(insertAndGetId).intValue();
      if (imageVersionId < 1) {
        log.error(String.format("Exception while creating image version due to invalid input, "
            + "imageVersionId: %d.", imageVersionId));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while creating image "
            + "version due to invalid input.");
      }
    } catch (final SQLException e) {
      log.error("Unable to create the image version metadata", e);
      String errorMessage = "";
      // TODO: Find a better way to get the error message. Currently apache common dbutils throws
      // sql exception for all the below error scenarios and error message contains complete
      // query as well, hence generic error message is thrown.
      if (e.getErrorCode() == SQL_ERROR_CODE_DUPLICATE_ENTRY) {
        errorMessage = "Reason: Duplicate key provided for one or more column(s).";
      }
      if (e.getErrorCode() == SQL_ERROR_CODE_DATA_TOO_LONG) {
        errorMessage = "Reason: Data too long for one or more column(s).";
      }
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while creating image "
          + "version metadata. " + errorMessage);
    }
    return imageVersionId;
  }

  @Override
  public List<ImageVersion> findImageVersions(final ImageMetadataRequest imageMetadataRequest)
      throws ImageMgmtException {
    List<ImageVersion> imageVersions = new ArrayList<>();
    try {
      final StringBuilder queryBuilder = new StringBuilder(SELECT_IMAGE_VERSION_BASE_QUERY);
      final List<Object> params = new ArrayList<>();
      // Add imageType in the query
      if (imageMetadataRequest.getParams().containsKey(ImageMgmtConstants.IMAGE_TYPE)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" lower(it.name) = ?");
        params.add(imageMetadataRequest.getParams().get(ImageMgmtConstants.IMAGE_TYPE).toString()
            .toLowerCase());
      }
      // Add imageVersion in the query if present
      if (imageMetadataRequest.getParams().containsKey(ImageMgmtConstants.IMAGE_VERSION)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" iv.version = ?");
        params.add(imageMetadataRequest.getParams().get(ImageMgmtConstants.IMAGE_VERSION));
      }
      // Add versionState in the query if present
      if (imageMetadataRequest.getParams().containsKey(ImageMgmtConstants.VERSION_STATE)) {
        queryBuilder.append(" AND ");
        queryBuilder.append(" iv.state = ?");
        final State versionState = (State) imageMetadataRequest.getParams()
            .get(ImageMgmtConstants.VERSION_STATE);
        params.add(versionState.getStateValue());
      }
      log.info("Image version get query : " + queryBuilder.toString());
      imageVersions = this.databaseOperator.query(queryBuilder.toString(),
          new FetchImageVersionHandler(), Iterables.toArray(params, Object.class));
    } catch (final SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException(ErrorCode.NOT_FOUND, "Exception while fetching image "
          + "version");
    }
    return imageVersions;
  }

  @Override
  public Optional<ImageVersion> getImageVersion(final String imageTypeName,
      final String imageVersion,
      final State versionState) throws ImageMgmtException {
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .addParam(IMAGE_TYPE, imageTypeName)
        .addParam(IMAGE_VERSION, imageVersion)
        .addParam(VERSION_STATE, versionState)
        .build();
    final List<ImageVersion> imageVersions = findImageVersions(imageMetadataRequest);
    return imageVersions != null && imageVersions.size() > 0 ? Optional.of(imageVersions.get(0))
        : Optional.empty();
  }

  @Override
  public Optional<ImageVersion> getImageVersion(final String imageTypeName,
      final String imageVersion) throws ImageMgmtException {
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .addParam(IMAGE_TYPE, imageTypeName)
        .addParam(IMAGE_VERSION, imageVersion)
        .build();
    final List<ImageVersion> imageVersions = findImageVersions(imageMetadataRequest);
    return imageVersions != null && imageVersions.size() > 0 ? Optional.of(imageVersions.get(0))
        : Optional.empty();
  }

  @Override
  public Optional<ImageVersion> getImageVersion(final String imageTypeName,
      final int versionId) throws ImageMgmtException {
    try {
      final List<ImageVersion> imageVersions = this.databaseOperator.query(
          SELECT_IMAGE_VERSION_BY_TYPE_AND_VERSION_ID, new FetchImageVersionHandler(),
          imageTypeName, versionId);
      return imageVersions != null && imageVersions.size() > 0 ? Optional.of(imageVersions.get(0))
          : Optional.empty();
    } catch (final SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException(ErrorCode.NOT_FOUND, String.format("Exception while fetching "
          + "image version for image type: %s, version id: %d", imageTypeName, versionId));
    }
  }

  @Override
  public List<ImageVersion> getActiveVersionByImageTypes(final Set<String> imageTypes)
      throws ImageMgmtException {
    List<ImageVersion> imageVersions = new ArrayList<>();
    try {
      // Add outer select clause
      final StringBuilder inClauseBuilder = new StringBuilder();
      for (int i = 0; i < imageTypes.size(); i++) {
        inClauseBuilder.append("?,");
      }
      inClauseBuilder.setLength(inClauseBuilder.length() - 1);
      final Map<String, String> valueMap = new HashMap<>();
      valueMap.put("image_types", inClauseBuilder.toString());

      final StringBuilder statesInClauseBuilder = new StringBuilder();
      statesInClauseBuilder.append("?");
      valueMap.put("version_states", statesInClauseBuilder.toString());

      final StrSubstitutor strSubstitutor = new StrSubstitutor(valueMap);
      final String query = strSubstitutor.replace(SELECT_LATEST_IMAGE_VERSION_QUERY);
      log.info("Image version getActiveVersionByImageTypes query : " + query);
      final List<Object> params = new ArrayList<>();
      final Set<String> imageTypesInLowerCase =
          imageTypes.stream().map(String::toLowerCase).collect(Collectors.toSet());
      log.info("imageTypesInLowerCase: " + imageTypesInLowerCase);
      // Add the state (active) and image types for the first inner query
      params.add(State.ACTIVE.getStateValue());
      params.addAll(imageTypesInLowerCase);
      // Add the state (active) and image types for the second inner query
      params.add(State.ACTIVE.getStateValue());
      params.addAll(imageTypesInLowerCase);
      imageVersions = this.databaseOperator.query(query,
          new FetchImageVersionHandler(), Iterables.toArray(params, Object.class));
      log.info("imageVersions {}", imageVersions);
    } catch (final SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while fetching image "
          + "version");
    }
    return imageVersions;
  }

  @Override
  public List<ImageVersion> getLatestNonActiveVersionByImageTypes(final Set<String> imageTypes)
      throws ImageMgmtException {
    List<ImageVersion> imageVersions = new ArrayList<>();
    try {
      // Add outer select clause
      final StringBuilder inClauseBuilder = new StringBuilder();
      for (int i = 0; i < imageTypes.size(); i++) {
        inClauseBuilder.append("?,");
      }
      inClauseBuilder.setLength(inClauseBuilder.length() - 1);
      final Map<String, String> valueMap = new HashMap<>();
      valueMap.put("image_types", inClauseBuilder.toString());

      final StringBuilder statesInClauseBuilder = new StringBuilder();
      for (int i = 0; i < State.getNonActiveStateValues().size(); i++) {
        statesInClauseBuilder.append("?,");
      }
      statesInClauseBuilder.setLength(statesInClauseBuilder.length() - 1);
      valueMap.put("version_states", statesInClauseBuilder.toString());

      final StrSubstitutor strSubstitutor = new StrSubstitutor(valueMap);
      final String query = strSubstitutor.replace(SELECT_LATEST_IMAGE_VERSION_QUERY);
      log.info("Image version getActiveVersionByImageTypes query : " + query);
      final List<Object> params = new ArrayList<>();
      final Set<String> imageTypesInLowerCase =
          imageTypes.stream().map(String::toLowerCase).collect(Collectors.toSet());
      log.info("imageTypesInLowerCase: " + imageTypesInLowerCase);
      // Add the states (new/unstable/deprecated)  and image types for the first inner query
      params.addAll(State.getNonActiveStateValues());
      params.addAll(imageTypesInLowerCase);
      // Add the states (new/unstable/deprecated)  and image types for the second inner query
      params.addAll(State.getNonActiveStateValues());
      params.addAll(imageTypesInLowerCase);
      imageVersions = this.databaseOperator.query(query,
          new FetchImageVersionHandler(), Iterables.toArray(params, Object.class));
      log.info("imageVersions {}", imageVersions);
    } catch (final SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while fetching image "
          + "version");
    }
    return imageVersions;
  }

  @Override
  public void updateImageVersion(final ImageVersion imageVersionRequest)
      throws ImageMgmtException {
    try {
      final List<Object> params = new ArrayList<>();
      final StringBuilder queryBuilder = new StringBuilder("update image_versions iv, image_types"
          + " it set ");
      if (imageVersionRequest.getPath() != null) {
        queryBuilder.append(" iv.path = ?, ");
        params.add(imageVersionRequest.getPath());
      }
      if (imageVersionRequest.getDescription() != null) {
        queryBuilder.append(" iv.description = ?, ");
        params.add(imageVersionRequest.getDescription());
      }
      if (imageVersionRequest.getState() != null) {
        queryBuilder.append(" iv.state = ?, ");
        params.add(imageVersionRequest.getState().getStateValue());
      }
      queryBuilder.append(" iv.modified_by = ?, iv.modified_on = ?");
      params.add(imageVersionRequest.getModifiedBy());
      params.add(Timestamp.valueOf(LocalDateTime.now()));
      queryBuilder.append(" where iv.id = ? and iv.type_id = it.id and it.name = ?");
      params.add(imageVersionRequest.getId());
      params.add(imageVersionRequest.getName());
      int updateCount = 0;
      if (imageVersionRequest.getState() != State.ACTIVE) {
        updateCount = this.databaseOperator.update(queryBuilder.toString(),
            Iterables.toArray(params, Object.class));
      } else {
        // If the state is being set to ACTIVE then only the current image version can be set
        // to ACTIVE and the rest of the image versions with ACTIVE state must be set to STABLE.
        final SQLTransaction<Integer> setCurrentVersionActive = transOperator -> {
          // Set all the current active versions to STABLE
          final String setStable = "update image_versions iv, image_types it "
              + "set iv.state = ?, iv.modified_on = ? "
              + "where iv.type_id = it.id and it.name = ? and iv.state = ?";
          final List<Object> setStableParams = new ArrayList<>();
          setStableParams.add(State.STABLE.getStateValue());
          setStableParams.add(Timestamp.valueOf(LocalDateTime.now()));
          setStableParams.add(imageVersionRequest.getName());
          setStableParams.add(State.ACTIVE.getStateValue());
          transOperator.update(setStable, Iterables.toArray(setStableParams,
              Object.class));

          // Now set the current version to ACTIVE
          final int count = transOperator.update(queryBuilder.toString(),
              Iterables.toArray(params, Object.class));
          transOperator.getConnection().commit();
          return count;
        };
        updateCount = this.databaseOperator.transaction(setCurrentVersionActive);
      }
      if (updateCount < 1) {
        log.error(String.format("Exception while updating image version due to invalid input, "
            + "updateCount: %d.", updateCount));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while updating image "
            + "version due to invalid input.");
      }
    } catch (final SQLException ex) {
      log.error("Exception while updating image version ", ex);
      String errorMessage = "";
      // TODO: Find a better way to get the error message. Currently apache common dbutils throws
      // sql exception for all the below error scenarios and error message contains complete
      // query as well, hence generic error message is thrown.
      if (ex.getErrorCode() == SQL_ERROR_CODE_DATA_TOO_LONG) {
        errorMessage = "Reason: Data too long for one or more column(s).";
      }
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while updating image "
          + "version");
    }
  }

  @Override
  public boolean isInvalidVersion(final String imageTypeName, final String version)
      throws ImageMgmtException {
    try {
      return this.databaseOperator.query(
          SELECT_INVALID_IMAGE_VERSION_QUERY, new CheckInvalidImageVersionHandler(),
          imageTypeName.toLowerCase(), version, State.NEW.getStateValue(),
          State.ACTIVE.getStateValue());
    } catch (final SQLException ex) {
      log.error("Exception while verifying whether image version is invalid or not. ", ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("Exception while "
              + "checking if the image version: %s  for image type: %s is valid or not", version,
          imageTypeName));
    }
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

  /**
   * ResultSetHandler implementation class for fetching image version
   */
  public static class CheckInvalidImageVersionHandler implements ResultSetHandler<Boolean> {

    @Override
    public Boolean handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return true;
      }
      return rs.getBoolean("is_invalid");
    }
  }
}
