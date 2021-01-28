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

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampup;
import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageRampupPlan;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAO Implementation for accessing image rampup plan and rampups for an image type. This class
 * contains implementation for methods such as create, get, update etc. for image rampups.
 */
@Singleton
public class ImageRampupDaoImpl implements ImageRampupDao {

  private static final Logger log = LoggerFactory.getLogger(ImageRampupDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  private final ImageTypeDao imageTypeDao;

  private final ImageVersionDao imageVersionDao;

  private static final String INSERT_IMAGE_RAMPUP_PLAN_QUERY =
      "insert into image_rampup_plan ( name, "
          + "description, type_id, active, created_by, created_on, modified_by, modified_on) "
          + "values (?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String INSERT_IMAGE_RAMPUP_QUERY = "insert into image_rampup "
      + "( plan_id, version_id, rampup_percentage, stability_tag, created_by, created_on, "
      + "modified_by, modified_on) values (?, ?, ?, ?, ?, ?, ?, ?)";

  /*
   * This query selects the active image rampup plan by joining image types. There shold be only one
   * active image rampup plan. Order by clause and limit will definitely ensure that one active plan
   * is selected in case there are more active entries due to bug. But ideally this should not be
   * the case.
   */
  private static final String SELECT_IMAGE_RAMPUP_ACTIVE_PLAN_QUERY = "select irp.id, irp.name, "
      + "irp.description, irp.active, it.name image_type_name, irp.created_on, irp.created_by, "
      + "irp.modified_on, irp.modified_by from image_rampup_plan irp, image_types it where "
      + "irp.type_id = it.id and lower(it.name) = ? and irp.active = ? order by irp.id desc "
      + "limit 1";

  private static final String DEACTIVATE_ACTIVE_RAMPUP_PLAN_QUERY = "update image_rampup_plan "
      + "set active = ? where id = ?";

  // This query selects rampup records for a given impage rampup plan
  private static final String SELECT_IMAGE_RAMPUP_QUERY = "select ir.id, ir.plan_id, iv.version, "
      + "ir.rampup_percentage, ir.stability_tag, ir.created_on, ir.created_by, ir.modified_on, "
      + "ir.modified_by from image_versions iv, image_rampup_plan irp, image_rampup ir where "
      + "irp.id = ir.plan_id and iv.id = ir.version_id and irp.id = ?";

  /*
   * This query selects rampup records for all the active image types.
   */
  private static final String SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY = "select ir.id, ir.plan_id, "
      + "iv.version image_version, ir.rampup_percentage, ir.stability_tag, it.name "
      + "image_type_name, ir.created_on, ir.created_by, ir.modified_on, ir.modified_by from "
      + "image_types it, image_versions iv, image_rampup_plan irp, image_rampup ir where "
      + "irp.id = ir.plan_id and iv.id = ir.version_id and irp.type_id = it.id and irp.active = ? "
      + "and iv.type_id = it.id and it.active = ?";


  @Inject
  public ImageRampupDaoImpl(final DatabaseOperator databaseOperator,
      final ImageTypeDao imageTypeDao,
      final ImageVersionDao imageVersionDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
  }

  /**
   * This method creates ramp up plan along with rampup details. Basically this method does bunch of
   * things. 1. Get the image type metadata. If not found throws exception. 2. Get the active rampup
   * plan for the specified image type. 3. If active ramup plan already exists - a. The user has set
   * activate plan to "true" for the new plan, the existing plan will be deactivated. This
   * deactivation of existing plan will happen automatically. b. The user has set activate plan to
   * "false" for the new plan, the user will be thrown with an error stating that there is an active
   * plan and to create and active a new plan, activate plan must be set to "true" in the input. 4.
   * Insert the new rampup plan for the image type in the image_rampup_plan table. 5. Insert the
   * associated image version, rampup percentage etc. in the image_rampup table. Also check if the
   * specified version is valid and exists. Otherwise, fail the whole transaction. Here is the
   * example input for ImageRampupPlanRequest { "planName": "Rampup plan 1", "imageType":
   * "spark_job", "description": "Ramp up for spark job", "activatePlan": true, "imageRampups": [ {
   * "imageVersion": "1.6.2", "rampupPercentage": "70" }, { "imageVersion": "1.6.1",
   * "rampupPercentage": "30" } ] }
   */
  @Override
  public int createImageRampupPlan(final ImageRampupPlan imageRampupPlan) {
    final ImageType imageType = this.imageTypeDao
        .getImageTypeByName(imageRampupPlan.getImageTypeName())
        .orElseThrow(() -> new ImageMgmtDaoException(ErrorCode.NOT_FOUND, String.format("Unable to"
                + " fetch image type metadata. Invalid image type: %s.",
            imageRampupPlan.getImageTypeName())));
    final SQLTransaction<Long> insertAndGetRampupPlanId = transOperator -> {
      // Fetch the active rampup plan for the image type
      final Optional<ImageRampupPlan> optionalImageRampupPlan =
          getActiveImageRampupPlan(imageType.getName());
      /*
       * If active rampup plan is already present and the activatePlan is set to true for the new
       * plan, deactivate the current plan as only one rampup plan will be active at a time.
       * Otherwise, appropriate error message will be thrown.
       */
      if (optionalImageRampupPlan.isPresent()) {
        if (imageRampupPlan.getForceActivatePlan() != null && imageRampupPlan.getForceActivatePlan()) {
          transOperator.update(DEACTIVATE_ACTIVE_RAMPUP_PLAN_QUERY,
              false, optionalImageRampupPlan.get().getId());
        } else {
          throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("Existing plan: %s "
                  + "is active for image type: %s. To create and activate a new plan, deactivate "
                  + "the existing plan. Alternatively, set forceActivatePlan to true to forcefully "
                  + "deactivate the existing plan and proceed with the new plan creation.",
              optionalImageRampupPlan.get().getPlanName(),
              imageRampupPlan.getImageTypeName()));
        }
      }
      // Passing timestamp from the code base and can be formatted accordingly based on timezone
      final Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
      // insert the new rampup plan
      transOperator.update(INSERT_IMAGE_RAMPUP_PLAN_QUERY, imageRampupPlan.getPlanName(),
          imageRampupPlan.getDescription(), imageType.getId(), true,
          imageRampupPlan.getCreatedBy(), currentTimestamp,
          imageRampupPlan.getModifiedBy(), currentTimestamp);
      final int rampupPlanId = Long.valueOf(transOperator.getLastInsertId()).intValue();
      // insert the rampups for the new plan
      if (imageRampupPlan.getImageRampups() != null
          && imageRampupPlan.getImageRampups().size() > 0) {
        for (final ImageRampup imageRampupRequest : imageRampupPlan
            .getImageRampups()) {
          // The image version which is marked as NEW can be picked for rampup up
          final Optional<ImageVersion> optionalImageVersion = this.imageVersionDao
              .getImageVersion(imageType.getName(), imageRampupRequest.getImageVersion());
          if (optionalImageVersion.isPresent()) {
            ImageVersion imageVersion = optionalImageVersion.get();
            if(!(State.NEW.equals(imageVersion.getState()) ||
                State.ACTIVE.equals(imageVersion.getState()))) {
              throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("The image "
                      + "versions with state NEW, ACTIVE can be selected for rampup. The "
                      + "image version: %s for image type: %s has version state: %s.",
                  imageRampupRequest.getImageVersion(), imageType.getName(),
                  imageVersion.getState().name()));
            }
            // During rampup plan creation all the verions will be marked as EXPERIMENTAL by
            // default in the image_rampup table. During the course of rampup, the version can be
            // marked as either STABLE or UNSTABLE using uddate API
            transOperator.update(INSERT_IMAGE_RAMPUP_QUERY, rampupPlanId,
                imageVersion.getId(), imageRampupRequest.getRampupPercentage(),
                StabilityTag.EXPERIMENTAL.getTagName(), imageType.getCreatedBy(), currentTimestamp,
                imageType.getModifiedBy(), currentTimestamp);
          } else {
            throw new ImageMgmtDaoException(ErrorCode.NOT_FOUND, String.format("Unable to get the "
                    + "image version: %s for image type: %s.  ",
                imageRampupRequest.getImageVersion(), imageType.getName()));
          }
        }
      }
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    int imageRampupPlanId = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      imageRampupPlanId = this.databaseOperator.transaction(insertAndGetRampupPlanId).intValue();
      if (imageRampupPlanId < 1) {
        log.error(String.format("Exception while creating image rampup plan for image type: %s, "
                + "imageRampupPlanId: %d.", imageRampupPlan.getImageTypeName(),
            imageRampupPlanId));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            String.format("Exception while creating image rampup plan for image type: %s, "
                    + "imageRampupPlanId: %d.", imageRampupPlan.getImageTypeName(),
                imageRampupPlanId));
      }
    } catch (final SQLException e) {
      log.error("Unable to create the image version metadata", e);
      String errorMessage = "";
      // TODO: Find a better way to get the error message. Currently apache common dbutils throws
      // sql exception for all the below error scenarios and error message contains complete
      // query as well, hence generic error message is thrown.
      if (e.getErrorCode() == SQL_ERROR_CODE_DATA_TOO_LONG) {
        errorMessage = "Reason: Data too long for one or more column(s).";
      }
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while creating image "
          + "version metadata");
    }
    return imageRampupPlanId;
  }

  @Override
  public Optional<ImageRampupPlan> getActiveImageRampupPlan(final String imageTypeName,
      final boolean fetchRampup)
      throws ImageMgmtException {
    return fetchRampup ? getActiveImageRampupPlanAndRampup(imageTypeName) :
        getActiveImageRampupPlan(imageTypeName);
  }

  private Optional<ImageRampupPlan> getActiveImageRampupPlan(final String imageTypeName)
      throws ImageMgmtException {
    try {
      return this.databaseOperator.query(SELECT_IMAGE_RAMPUP_ACTIVE_PLAN_QUERY,
          new FetchImageRampupPlanHandler(), imageTypeName.toLowerCase(), true);
    } catch (final SQLException ex) {
      log.error(String.format("Exception while fetching active rampup plan for image type: %s. ",
          imageTypeName), ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          String.format("Exception while fetching active rampup plan for image type: %s. ",
              imageTypeName));
    }
  }

  /**
   * Gets active rampup plan with rampup for the given image type
   *
   * @param imageTypeName
   * @return Optional<ImageRampupPlan>
   * @throws ImageMgmtException
   */
  private Optional<ImageRampupPlan> getActiveImageRampupPlanAndRampup(final String imageTypeName)
      throws ImageMgmtException {
    try {
      final Optional<ImageRampupPlan> imageRampupPlan = getActiveImageRampupPlan(imageTypeName);

      if (imageRampupPlan.isPresent()) {
        final List<ImageRampup> imageRampups = this.databaseOperator
            .query(SELECT_IMAGE_RAMPUP_QUERY,
                new FetchImageRampupHandler(), imageRampupPlan.get().getId());
        imageRampupPlan.get().setImageRampups(imageRampups);
      }
      return imageRampupPlan;
    } catch (final SQLException ex) {
      log.error(String.format("Exception while fetching active rampup plan for image type: %s. ",
          imageTypeName), ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          String.format("Exception while fetching active rampup plan for image type: %s. ",
              imageTypeName));
    }
  }

  @Override
  public Map<String, List<ImageRampup>> getRampupForAllImageTypes()
      throws ImageMgmtException {
    try {
      return this.databaseOperator.query(SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY,
          new FetchImageTypeRampupHandler(), true, true);
    } catch (final SQLException ex) {
      log.error("Exception while fetching rampup for image types.", ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          "Exception while fetching rampup for image types.");
    }
  }

  @Override
  public Map<String, List<ImageRampup>> getRampupByImageTypes(final Set<String> imageTypes)
      throws ImageMgmtException {
    try {
      if (imageTypes == null || imageTypes.isEmpty()) {
        return getRampupForAllImageTypes();
      }
      final StringBuilder queryBuilder = new StringBuilder(SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY);
      queryBuilder.append(" and ir.stability_tag in ( ?, ? ) ");
      queryBuilder.append(" and lower(it.name) in ( ");
      for (int i = 0; i < imageTypes.size() - 1; i++) {
        queryBuilder.append("? ,");
      }
      queryBuilder.append("? )");
      log.info("fetchRampupByImageTypes query: " + queryBuilder.toString());
      final List<Object> params = new ArrayList<>();
      // Select active image rampup plan
      params.add(Boolean.TRUE);
      // Select active image type
      params.add(Boolean.TRUE);
      // Select only EXPERIMENTAL and STABLE versions that are being ramped up. Ignore the
      // UNSTABLE version.
      params.add(StabilityTag.EXPERIMENTAL.getTagName());
      params.add(StabilityTag.STABLE.getTagName());
      params.addAll(imageTypes.stream().map(String::toLowerCase).collect(Collectors.toSet()));
      return this.databaseOperator.query(queryBuilder.toString(),
          new FetchImageTypeRampupHandler(), Iterables.toArray(params, Object.class));
    } catch (final SQLException ex) {
      log.error(String.format("Exception while fetching rampup for image types: %s.", imageTypes),
          ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          String.format("Exception while fetching rampup for image types: %s.", imageTypes));
    }
  }

  @Override
  public void updateImageRampupPlan(final ImageRampupPlan imageRampupPlan)
      throws ImageMgmtException {
    // Select the active rampup plan and the rampups for the given image type
    final Optional<ImageRampupPlan> optionalImageRampupPlan =
        getActiveImageRampupPlanAndRampup(imageRampupPlan.getImageTypeName());
    if (optionalImageRampupPlan.isPresent()) {
      final ImageRampupPlan existingImageRampupPlan = optionalImageRampupPlan.get();
      /*
       * The below internal ID mapping is required because the update API image type for which
       * rampup plan and rampup needs to be updated. It is easy for the API user to pass just
       * image type. Other the API user needs ID of the rampup plan and all the IDs of the
       * rampups. Hence passing just image types simplifies API invocation part. As a result the
       * below code needs to update the internal ID mapping in the update request object so that
       * the necessary update can be done using all the IDs. For example, imageRampupPlanRequest
       * looks like this -
       * {
       *   "planName": "plan name"
       *   "imageTYpe": "spark_job",
       *   "activatePlan": true,
       *   "imageRampups": [
       *     {
       *       "imageVersion": "1.6.2",
       *       "rampupPercentage": "80",
       *       "stabilityTag": STABLE
       *     },
       *     {
       *       "imageVersion": "1.6.1",
       *       "rampupPercentage": "20",
       *       "stabilityTag": STABLE
       *     }
       *   ]
       * }
       */
      // Set the ID of the plan to be updated.
      imageRampupPlan.setId(existingImageRampupPlan.getId());
      /*
       * Update the internal ID (Key of image_rampup table) for each version present in the rampup
       * request so that update can be done using the ID. The below code will prepare version to
       * ID map based on the version and id available in the image_rampup table.
       */
      if (!CollectionUtils.isEmpty(imageRampupPlan.getImageRampups())) {
        final Map<String, Integer> versionIdKeyMap = new HashMap<>();
        for (final ImageRampup imageRampup : existingImageRampupPlan.getImageRampups()) {
          versionIdKeyMap.put(imageRampup.getImageVersion(), imageRampup.getId());
        }

        // Use the version to id map created above to update the ID of each ramp up record.
        for (final ImageRampup imageRampup : imageRampupPlan
            .getImageRampups()) {
          if (versionIdKeyMap.containsKey(imageRampup.getImageVersion())) {
            imageRampup.setId(versionIdKeyMap.get(imageRampup.getImageVersion()));
          } else {
            // Throw exception if invalid version is specified in the input request
            throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("Invalid version: "
                + "%s specified in the image rampup input.", imageRampup.getImageVersion()));
          }
        }
      }
      // Update image_rampup_plan and image_rampup table using the corresponding ID (key) and the
      // information to be updated.
      final SQLTransaction<Integer> updateImageRamupTransaction = transOperator -> {
        // update image rampup plan
        updateImageRampupPlanInternal(imageRampupPlan);
        // update each rampup
        if (!CollectionUtils.isEmpty(imageRampupPlan.getImageRampups())) {
          for (final ImageRampup imageRampup : imageRampupPlan
              .getImageRampups()) {
            updateImageRampup(imageRampup);
          }
        }
        transOperator.getConnection().commit();
        return 1;
      };
      try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
        this.databaseOperator.transaction(updateImageRamupTransaction);
      } catch (final SQLException e) {
        log.error(String.format("Unable to update the image rampup details for image type: %s",
            imageRampupPlan.getImageTypeName()), e);
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            String.format("Unable to update the image rampup details for image type: %s",
                imageRampupPlan.getImageTypeName()));
      }
    } else {
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          String.format("There is no active rampup plan found for image "
              + "type: %s.", imageRampupPlan.getImageTypeName()));
    }
  }

  /**
   * This method updates image rampup plan for an image type and the plan can be either made active
   * or inactive.
   *
   * @param imageRampupPlan
   * @throws ImageMgmtException
   */
  private void updateImageRampupPlanInternal(final ImageRampupPlan imageRampupPlan)
      throws ImageMgmtException {
    try {
      final List<Object> params = new ArrayList<>();
      final StringBuilder queryBuilder = new StringBuilder("update image_rampup_plan set ");
      // As update is allowed only for active plan, if activatePlan is false then deactivate the
      // current plan.
      if ((imageRampupPlan.getActivatePlan() != null && !imageRampupPlan.getActivatePlan()) ||
          (imageRampupPlan.getForceActivatePlan() != null && !imageRampupPlan.getForceActivatePlan())) {
        queryBuilder.append(" active = ?, ");
        params.add(Boolean.FALSE);
      }
      queryBuilder.append(" modified_by = ?, modified_on = ?");
      params.add(imageRampupPlan.getModifiedBy());
      params.add(Timestamp.valueOf(LocalDateTime.now()));
      queryBuilder.append(" where id = ? ");
      params.add(imageRampupPlan.getId());
      final int updateCount = this.databaseOperator
          .update(queryBuilder.toString(), Iterables.toArray(params, Object.class));
      if (updateCount < 1) {
        log.error(String.format("Exception while updating image rampup plan for image type: %s, "
                + "updateCount: %d. ",
            imageRampupPlan.getImageTypeName(), updateCount));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("Exception while "
                + "updating image rampup plan for image type: %s. ",
            imageRampupPlan.getImageTypeName()));
      }
    } catch (final SQLException ex) {
      log.error(String.format("Exception while updating image rampup plan for image type: %s. ",
          imageRampupPlan.getImageTypeName()), ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, String.format("Exception while "
              + "updating image rampup plan for image type: %s. ",
          imageRampupPlan.getImageTypeName()));
    }
  }

  /**
   * This method up updates image rampup for an image type. The information such as version rampup
   * percentage, stability tag etc. can be updated.
   *
   * @param imageRampup
   * @throws ImageMgmtException
   */
  private void updateImageRampup(final ImageRampup imageRampup)
      throws ImageMgmtException {
    try {
      final List<Object> params = new ArrayList<>();
      final StringBuilder queryBuilder = new StringBuilder("update image_rampup set ");
      if (imageRampup.getRampupPercentage() != null) {
        queryBuilder.append(" rampup_percentage = ?, ");
        params.add(imageRampup.getRampupPercentage());
      }
      if (imageRampup.getStabilityTag() != null) {
        queryBuilder.append(" stability_tag = ?, ");
        params.add(imageRampup.getStabilityTag().getTagName());
      }
      queryBuilder.append(" modified_by = ?, modified_on = ?");
      params.add(imageRampup.getModifiedBy());
      params.add(Timestamp.valueOf(LocalDateTime.now()));
      queryBuilder.append(" where id = ? ");
      params.add(imageRampup.getId());
      final int updateCount = this.databaseOperator
          .update(queryBuilder.toString(), Iterables.toArray(params, Object.class));
      if (updateCount < 1) {
        log.error(String.format("Exception while updating image rampup details for plan id: %s, "
                + "updateCount: %d. ",
            imageRampup.getPlanId(), updateCount));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            String.format("Exception while updating image rampup details for plan id: %s. ",
                imageRampup.getPlanId()));
      }
    } catch (final SQLException ex) {
      log.error(String.format("Exception while updating image rampup details for plan id: %s. ",
          imageRampup.getPlanId()), ex);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
          String.format("Exception while updating image rampup details for plan id: %s. ",
              imageRampup.getPlanId()));
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image rampup plan
   */
  public static class FetchImageRampupPlanHandler implements
      ResultSetHandler<Optional<ImageRampupPlan>> {

    @Override
    public Optional<ImageRampupPlan> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Optional.empty();
      }
      final ImageRampupPlan imageRampupPlan = new ImageRampupPlan();
      do {
        final int id = rs.getInt("id");
        final String name = rs.getString("name");
        final String description = rs.getString("description");
        final String imageTypeName = rs.getString("image_type_name");
        final boolean active = rs.getBoolean("active");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        imageRampupPlan.setId(id);
        imageRampupPlan.setPlanName(name);
        imageRampupPlan.setDescription(description);
        imageRampupPlan.setActive(active);
        imageRampupPlan.setImageTypeName(imageTypeName);
        imageRampupPlan.setCreatedOn(createdOn);
        imageRampupPlan.setCreatedBy(createdBy);
        imageRampupPlan.setModifiedBy(modifiedBy);
        imageRampupPlan.setModifiedOn(modifiedOn);
      } while (rs.next());
      return Optional.of(imageRampupPlan);
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image rampup
   */
  public static class FetchImageRampupHandler implements
      ResultSetHandler<List<ImageRampup>> {

    @Override
    public List<ImageRampup> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageRampup> imageRampups = new ArrayList<>();
      do {
        final int id = rs.getInt("id");
        final int planId = rs.getInt("plan_id");
        final String imageVersion = rs.getString("version");
        final int rampupPercentage = rs.getInt("rampup_percentage");
        final String stabilityTag = rs.getString("stability_tag");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageRampup imageRampup = new ImageRampup();
        imageRampup.setId(id);
        imageRampup.setPlanId(planId);
        imageRampup.setImageVersion(imageVersion);
        imageRampup.setRampupPercentage(rampupPercentage);
        imageRampup.setStabilityTag(StabilityTag.fromTagName(stabilityTag));
        imageRampup.setCreatedOn(createdOn);
        imageRampup.setCreatedBy(createdBy);
        imageRampup.setModifiedBy(modifiedBy);
        imageRampup.setModifiedOn(modifiedOn);
        imageRampups.add(imageRampup);
      } while (rs.next());
      log.debug("Fetched imageRampups:" + imageRampups);
      return imageRampups;
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image rampup details for given image types
   */
  public static class FetchImageTypeRampupHandler implements
      ResultSetHandler<Map<String, List<ImageRampup>>> {

    @Override
    public Map<String, List<ImageRampup>> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }
      final Map<String, List<ImageRampup>> imageRampupMap = new LinkedHashMap<>();
      do {
        final int id = rs.getInt("id");
        final int planId = rs.getInt("plan_id");
        final String imageTypeName = rs.getString("image_type_name");
        final String imageVersion = rs.getString("image_version");
        final int rampupPercentage = rs.getInt("rampup_percentage");
        final String stabilityTag = rs.getString("stability_tag");
        final String createdOn = rs.getString("created_on");
        final String createdBy = rs.getString("created_by");
        final String modifiedOn = rs.getString("modified_on");
        final String modifiedBy = rs.getString("modified_by");
        final ImageRampup imageRampup = new ImageRampup();
        imageRampup.setId(id);
        imageRampup.setImageVersion(imageVersion);
        imageRampup.setRampupPercentage(rampupPercentage);
        imageRampup.setStabilityTag(StabilityTag.fromTagName(stabilityTag));
        imageRampup.setCreatedOn(createdOn);
        imageRampup.setCreatedBy(createdBy);
        imageRampup.setModifiedBy(modifiedBy);
        imageRampup.setModifiedOn(modifiedOn);
        if (imageRampupMap.containsKey(imageTypeName)) {
          imageRampupMap.get(imageTypeName).add(imageRampup);
        } else {
          final List<ImageRampup> imageRampupList = new ArrayList<>();
          imageRampupList.add(imageRampup);
          imageRampupMap.put(imageTypeName, imageRampupList);
        }
      } while (rs.next());
      return imageRampupMap;
    }
  }
}
