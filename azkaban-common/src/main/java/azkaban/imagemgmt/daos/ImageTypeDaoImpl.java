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

import static azkaban.imagemgmt.utils.ErroCodeConstants.SQL_ERROR_CODE_DATA_TOO_LONG;
import static azkaban.imagemgmt.utils.ErroCodeConstants.SQL_ERROR_CODE_DUPLICATE_ENTRY;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAO Implementation for accessing image types. This class contains implementation for creating
 * image type, getting image type metadata etc.
 */
@Singleton
public class ImageTypeDaoImpl implements ImageTypeDao {

  private static final Logger log = LoggerFactory.getLogger(ImageTypeDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  static String INSERT_IMAGE_TYPE =
      "insert into image_types ( name, description, active, deployable, created_by, "
          + "created_on, modified_by, modified_on )"
          + " values (?, ?, ?, ?, ?, ?, ?, ?)";
  static String INSERT_IMAGE_OWNERSHIP =
      "insert into image_ownerships ( type_id, owner, role, created_by, created_on, modified_by, "
          + "modified_on ) values (?, ?, ?, ?, ?, ?, ?)";

  @Inject
  public ImageTypeDaoImpl(final DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  @Override
  public int createImageType(final ImageType imageType) {
    final SQLTransaction<Integer> insertAndGetId = transOperator -> {
      // insert image type record
      // Passing timestamp from the code base and can be formatted accordingly based on timezone
      final Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
      transOperator.update(INSERT_IMAGE_TYPE, imageType.getName(),
          imageType.getDescription(), true, imageType.getDeployable().getName(),
          imageType.getCreatedBy(), currentTimestamp, imageType.getModifiedBy(), currentTimestamp);
      final int imageTypeId = Long.valueOf(transOperator.getLastInsertId()).intValue();
      // Insert ownerships record if present
      if (imageType.getOwnerships() != null && imageType.getOwnerships().size() > 0) {
        for (final ImageOwnership imageOwnership : imageType.getOwnerships()) {
          transOperator.update(INSERT_IMAGE_OWNERSHIP, imageTypeId, imageOwnership.getOwner(),
              imageOwnership.getRole().name(), imageType.getCreatedBy(), currentTimestamp,
              imageType.getModifiedBy(), currentTimestamp);
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
      imageTypeId = this.databaseOperator.transaction(insertAndGetId);
      if (imageTypeId < 1) {
        log.error(String.format("Exception while creating image type due to invalid input, "
            + "imageTypeId: %d.", imageTypeId));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while creating image "
            + "type due to invalid input.");
      }
      log.info("Created image type id :" + imageTypeId);
    } catch (final SQLException e) {
      log.error("Unable to create the image type metadata", e);
      String errorMessage = "";
      // TODO: Find a better way to get the error message. Currently apache common dbutils
      // throws sql exception for all the below error scenarios and error message contains
      // complete query as well, hence generic error message is thrown.
      if (e.getErrorCode() == SQL_ERROR_CODE_DUPLICATE_ENTRY) {
        errorMessage = "Reason: Duplicate key provided for one or more column(s).";
      }
      if (e.getErrorCode() == SQL_ERROR_CODE_DATA_TOO_LONG) {
        errorMessage = "Reason: Data too long for one or more column(s).";
      }
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception occurred while creating "
          + "image type metadata. " + errorMessage);
    }
    return imageTypeId;
  }

  @Override
  public ImageType getImageTypeWithOwnershipsById(final String id) {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    List<ImageType> imageTypes = new ArrayList<>();
    try {
      imageTypes = this.databaseOperator.query(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_ID,
          fetchImageTypeHandler, id);
      if (imageTypes == null || imageTypes.isEmpty()) {
        throw new ImageMgmtDaoException(ErrorCode.NOT_FOUND,
            "Failed to find image type for " + id);
      }
      if (imageTypes.size() > 1) {
        throw new ImageMgmtDaoException(ErrorCode.UNPROCESSABLE_ENTITY, "The request for image "
            + "type with id " + id + " unexpectedly returned more than one result!");
      }
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_ID + " failed.", ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Unable to get image type for : " + id);
    }
    ImageType imageType = imageTypes.get(0);
    imageType.setOwnerships(getImageTypeOwnership(imageType.getName()));
    return imageType;
  }


  @Override
  public Optional<ImageType> getImageTypeByName(final String name) {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    List<ImageType> imageTypes = new ArrayList<>();
    try {
      imageTypes = this.databaseOperator
          .query(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_NAME, fetchImageTypeHandler,
              name.toLowerCase());
      // Check if there are more then one image types for a given name. If so throw exception
      if (imageTypes.size() > 1) {
        throw new ImageMgmtDaoException(ErrorCode.UNPROCESSABLE_ENTITY,
            "Failed to get image type by "
                + "name. Can't have more that one image type record for a given type with name : "
                + name);
      }
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_NAME + " failed.", ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Unable to get image type metadata for image type : " + name);
    }
    return imageTypes.isEmpty() ? Optional.empty() : Optional.of(imageTypes.get(0));
  }

  @Override
  public Optional<ImageType> getImageTypeWithOwnershipsByName(final String name)
      throws ImageMgmtException {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    List<ImageType> imageTypes = new ArrayList<>();
    try {
      imageTypes = this.databaseOperator
          .query(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_NAME, fetchImageTypeHandler,
              name.toLowerCase());
      // Check if there are more then one image types for a given name. If so throw exception
      if (imageTypes != null && imageTypes.size() > 1) {
        log.error(
            "Failed to get image type with ownerships by name. Can't have more that one image type"
                + " record for a given type with name : " + name);
        throw new ImageMgmtDaoException(ErrorCode.UNPROCESSABLE_ENTITY, "Failed to get image type"
            + " with "
            + "ownerships by name. Can't have more that one image type record for a given type "
            + "with name : " + name);
      }
      if (!imageTypes.isEmpty()) {
        final ImageType imageType = imageTypes.get(0);
        imageType.setOwnerships(getImageTypeOwnership(imageType.getName()));
      }
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_IMAGE_TYPE_BY_NAME + " failed.", ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Unable to fetch image type metadata for image type : " + name);
    }
    return imageTypes.isEmpty() ? Optional.empty() : Optional.of(imageTypes.get(0));
  }

  @Override
  public List<ImageType> getAllImageTypes() throws ImageMgmtException {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    try {
      return this.databaseOperator
          .query(FetchImageTypeHandler.FETCH_ALL_IMAGE_TYPES, fetchImageTypeHandler, true);
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_ALL_IMAGE_TYPES + " failed.", ex);
      throw new ImageMgmtDaoException(
          "Unable to fetch all image type metadata ");
    }
  }

  @Override
  public List<ImageType> getAllImageTypesWithOwnerships() throws ImageMgmtException {
    final FetchImageTypeHandler fetchImageTypeHandler = new FetchImageTypeHandler();
    List<ImageType> imageTypes = new ArrayList<>();
    try {
      imageTypes = this.databaseOperator
          .query(FetchImageTypeHandler.FETCH_ALL_IMAGE_TYPES, fetchImageTypeHandler, true);
      // Get the image type names
      final Set<String> imageTypeNames = new HashSet<>();
      for (final ImageType imageType : imageTypes) {
        imageTypeNames.add(imageType.getName().toLowerCase());
      }
      // Build in clause for fetching the ownership information for all the image types
      final StringBuilder inClauseBuilder = new StringBuilder();
      for (int i = 0; i < imageTypeNames.size(); i++) {
        inClauseBuilder.append("?,");
      }
      inClauseBuilder.setLength(inClauseBuilder.length() - 1);
      final Map<String, String> valueMap = new HashMap<>();
      valueMap.put("image_types", inClauseBuilder.toString());
      final StrSubstitutor strSubstitutor = new StrSubstitutor(valueMap);
      final String query = strSubstitutor
          .replace(FetchAllImageOwnershipHandler.FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAMES);
      log.info("Fetch all image types ownership query : " + query);
      // Execute the query to get owners for all the image types.
      final Map<String, List<ImageOwnership>> imageTypeOwnershipMap = this.databaseOperator
          .query(query,
              new FetchAllImageOwnershipHandler(),
              imageTypeNames.toArray());
      // Set the ownership information to the image type metadata
      for (final ImageType imageType : imageTypes) {
        imageType.setOwnerships(imageTypeOwnershipMap.get(imageType.getName()));
      }
    } catch (final SQLException ex) {
      log.error(FetchImageTypeHandler.FETCH_ALL_IMAGE_TYPES + " failed.", ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Unable to fetch all image type metadata ");
    }
    return imageTypes;
  }

  /**
   * Gets ownership metadata based on image type name.
   *
   * @param imageTypeName
   * @return List<ImageOwnership>
   */
  private List<ImageOwnership> getImageTypeOwnership(final String imageTypeName) {
    final FetchImageOwnershipHandler fetchImageOwnershipHandler = new FetchImageOwnershipHandler();
    try {
      return this.databaseOperator
          .query(FetchImageOwnershipHandler.FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME,
              fetchImageOwnershipHandler, imageTypeName.toLowerCase());
    } catch (final SQLException ex) {
      log.error(FetchImageOwnershipHandler.FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME + " failed.",
          ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Unable to fetch ownership for image type : " + imageTypeName);
    }
  }

  /**
   * Gets ownership metadata based on image type name and user id.
   *
   * @param imageTypeName
   * @return Optional<ImageOwnership>
   */
  @Override
  public Optional<ImageOwnership> getImageTypeOwnership(final String imageTypeName,
      final String userId) {
    final FetchImageOwnershipHandler fetchImageOwnershipHandler = new FetchImageOwnershipHandler();
    List<ImageOwnership> imageOwnerships = new ArrayList<>();
    try {
      imageOwnerships = this.databaseOperator
          .query(FetchImageOwnershipHandler.FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME_AND_USER_ID,
              fetchImageOwnershipHandler, imageTypeName.toLowerCase(), userId);
    } catch (final SQLException ex) {
      log.error(FetchImageOwnershipHandler.FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME_AND_USER_ID
          + " failed.", ex);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR, String.format(
          "Unable to fetch ownership for image type: %s, user id: %s. ", imageTypeName, userId));
    }
    return imageOwnerships.isEmpty() ? Optional.empty() : Optional.of(imageOwnerships.get(0));
  }

  /**
   * ResultSetHandler implementation class for fetching image type
   */
  public static class FetchImageTypeHandler implements ResultSetHandler<List<ImageType>> {

    private static final String FETCH_IMAGE_TYPE_BY_NAME =
        "SELECT id, name, description, active, deployable, created_on, created_by, modified_on, "
            + "modified_by FROM image_types WHERE lower(name) = ?";
    private static final String FETCH_ALL_IMAGE_TYPES =
        "SELECT id, name, description, active, deployable, created_on, created_by, modified_on, "
            + "modified_by FROM image_types where active = ?";

    private static final String FETCH_IMAGE_TYPE_BY_ID =
        "SELECT id, name, description, active, deployable, created_on, created_by, modified_on, "
            + "modified_by FROM image_types WHERE id = ?";

    @Override
    public List<ImageType> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageType> imageTypes = new ArrayList<>();
      do {
        final int id = rs.getInt("id");
        final String name = rs.getString("name");
        final String description = rs.getString("description");
        final boolean active = rs.getBoolean("active");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageType imageType = new ImageType();
        imageType.setId(id);
        imageType.setName(name);
        imageType.setDescription(description);
        imageType.setCreatedOn(createdOn);
        imageType.setCreatedBy(createdBy);
        imageType.setModifiedOn(modifiedOn);
        imageType.setModifiedBy(modifiedBy);
        imageTypes.add(imageType);
      } while (rs.next());
      return imageTypes;
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image type
   */
  public static class FetchImageOwnershipHandler implements ResultSetHandler<List<ImageOwnership>> {

    private static final String FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME =
        "SELECT it.name, io.id, io.owner, io.role, io.created_by, io.created_on, io.modified_by, "
            + "io.modified_on FROM image_types it, image_ownerships io  WHERE it.id = io.type_id "
            + "and lower(it.name) = ?";

    private static final String FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAME_AND_USER_ID =
        "SELECT it.name, io.id, io.owner, io.role, io.created_by, io.created_on, io.modified_by, "
            + "io.modified_on FROM image_types it, image_ownerships io  WHERE it.id = io.type_id "
            + "and lower(it.name) = ? and io.owner = ?";

    @Override
    public List<ImageOwnership> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageOwnership> imageOwnerships = new ArrayList<>();
      do {
        final String name = rs.getString("name");
        final int id = rs.getInt("id");
        final String owner = rs.getString("owner");
        final String role = rs.getString("role");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageOwnership imageOwnership = new ImageOwnership();
        imageOwnership.setId(id);
        imageOwnership.setName(name);
        imageOwnership.setOwner(owner);
        imageOwnership.setRole(Role.valueOf(role));
        imageOwnership.setCreatedOn(createdOn);
        imageOwnership.setCreatedBy(createdBy);
        imageOwnership.setModifiedOn(modifiedOn);
        imageOwnership.setModifiedBy(modifiedBy);
        imageOwnerships.add(imageOwnership);
      } while (rs.next());
      return imageOwnerships;
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image type
   */
  public static class FetchAllImageOwnershipHandler implements ResultSetHandler<Map<String,
      List<ImageOwnership>>> {

    private static final String FETCH_IMAGE_OWNERSHIP_BY_IMAGE_TYPE_NAMES =
        "SELECT it.name, io.id, io.owner, io.role, io.created_by, io.created_on, io.modified_by, "
            + "io.modified_on FROM image_types it, image_ownerships io  WHERE it.id = io.type_id "
            + "and lower(it.name) in ( ${image_types} )";

    @Override
    public Map<String, List<ImageOwnership>> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }
      final Map<String, List<ImageOwnership>> imageTypeOwnershipMap = new HashMap<>();
      do {
        final String imageTypeName = rs.getString("name");
        final int id = rs.getInt("id");
        final String owner = rs.getString("owner");
        final String role = rs.getString("role");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageOwnership imageOwnership = new ImageOwnership();
        imageOwnership.setId(id);
        imageOwnership.setName(imageTypeName);
        imageOwnership.setOwner(owner);
        imageOwnership.setRole(Role.valueOf(role));
        imageOwnership.setCreatedOn(createdOn);
        imageOwnership.setCreatedBy(createdBy);
        imageOwnership.setModifiedOn(modifiedOn);
        imageOwnership.setModifiedBy(modifiedBy);
        if (imageTypeOwnershipMap.containsKey(imageTypeName)) {
          imageTypeOwnershipMap.get(imageTypeName).add(imageOwnership);
        } else {
          final List<ImageOwnership> imageOwnerships = new ArrayList<>();
          imageOwnerships.add(imageOwnership);
          imageTypeOwnershipMap.put(imageTypeName, imageOwnerships);
        }
      } while (rs.next());
      return imageTypeOwnershipMap;
    }
  }
}