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

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.exeception.ImageMgmtDaoException;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampup;
import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.models.ImageRampupPlanRequest;
import azkaban.imagemgmt.models.ImageRampupRequest;
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
import javax.inject.Inject;
import javax.inject.Singleton;
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
  /**
   * This query selects the active image rampup plan by joining image types. There shold be only one
   * active image rampup plan. Order by clause and limit will definitely ensure that one active plan
   * is selected in case there are more active entries due to bug. But ideally this should not be
   * the case.
   */
  private static final String SELECT_IMAGE_RAMPUP_ACTIVE_PLAN_QUERY = "select irp.id, irp.name, "
      + "irp.description, irp.active, it.name image_type_name, irp.created_on, irp.created_by, "
      + "irp.modified_on, irp.modified_by from image_rampup_plan irp, image_types it where "
      + "irp.type_id = it.id and it.name = ? and irp.active = ? order by irp.id desc limit 1";
  private static final String DEACTIVATE_ACTIVE_RAMPUP_PLAN_QUERY = "update image_rampup_plan "
      + "set active = ? where id = ?";
  // This query selects rampup records for a given impage rampup plan
  private static final String SELECT_IMAGE_RAMPUP_QUERY = "select ir.id, ir.plan_id, iv.version, "
      + "ir.rampup_percentage, ir.stability_tag, ir.created_on, ir.created_by, ir.modified_on, "
      + "ir.modified_by from image_versions iv, image_rampup_plan irp, image_rampup ir where "
      + "irp.id = ir.plan_id and iv.id = ir.version_id and irp.id = ?";
  /**
   * This query selects rampup records for all the active image types.
   */
  private static final String SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY = "select ir.id, ir.plan_id, "
      + "iv.version image_version, ir.rampup_percentage, ir.stability_tag, it.name "
      + "image_type_name, ir.created_on, ir.created_by, ir.modified_on, ir.modified_by from "
      + "image_types it, image_versions iv, image_rampup_plan irp, image_rampup ir where "
      + "irp.id = ir.plan_id and iv.id = ir.version_id and irp.type_id = it.id and irp.active = ? "
      + "and iv.type_id = it.id and it.active = ?";


  @Inject
  public ImageRampupDaoImpl(DatabaseOperator databaseOperator, ImageTypeDao imageTypeDao,
      ImageVersionDao imageVersionDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
  }

  @Override
  public int createImageRampupPlan(ImageRampupPlanRequest imageRampupPlanRequest) {
    /**
     * Here is the example input for ImageRampupPlanRequest
     * {
     *   "planName": "Rampup plan 1",
     *   "imageType": "spark_job",
     *   "description": "Ramp up for spark job",
     *   "activatePlan": true,
     *   "imageRampups": [
     *     {
     *       "imageVersion": "1.6.2",
     *       "rampupPercentage": "70"
     *     },
     *     {
     *       "imageVersion": "1.6.1",
     *       "rampupPercentage": "30"
     *     }
     *   ]
     * }
     */
    ImageType imageType = imageTypeDao
        .getImageTypeByName(imageRampupPlanRequest.getImageTypeName())
        .orElseThrow(() -> new ImageMgmtDaoException("Unable to fetch image type metadata. Invalid "
            + "image type : " + imageRampupPlanRequest.getImageTypeName()));
    SQLTransaction<Long> insertAndGetRampupPlanId = transOperator -> {
      // Fetch the active rampup plan for the image type
      Optional<ImageRampupPlan> optionalImageRampupPlan =
          getActiveImageRampupPlan(imageType.getName());
      /**
       * If active rampup plan is already present and the activatePlan is set to true for the new
       * plan, deactivate the current plan as only one rampup plan will be active at a time.
       * Otherwise, appropriate error message will be thrown.
       */
      if (optionalImageRampupPlan.isPresent()) {
        if (imageRampupPlanRequest.isActivatePlan()) {
          transOperator.update(DEACTIVATE_ACTIVE_RAMPUP_PLAN_QUERY,
              false, optionalImageRampupPlan.get().getId());
        } else {
          throw new ImageMgmtDaoException(String.format("Existing plan: %s is active for image "
                  + "type: %s. To create and activate a new plan set activatePlan option to true.",
              optionalImageRampupPlan.get().getPlanName(),
              imageRampupPlanRequest.getImageTypeName()));
        }
      }
      // Passing timestamp from the code base and can be formatted accordingly based on timezone
      Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
      // insert the new rampup plan
      transOperator.update(INSERT_IMAGE_RAMPUP_PLAN_QUERY, imageRampupPlanRequest.getPlanName(),
          imageRampupPlanRequest.getDescription(), imageType.getId(), true,
          imageRampupPlanRequest.getCreatedBy(), currentTimestamp,
          imageRampupPlanRequest.getModifiedBy(), currentTimestamp);
      int rampupPlanId = Long.valueOf(transOperator.getLastInsertId()).intValue();
      // insert the rampups for the new plan
      if (imageRampupPlanRequest.getImageRampups() != null
          && imageRampupPlanRequest.getImageRampups().size() > 0) {
        for (ImageRampupRequest imageRampupRequest : imageRampupPlanRequest
            .getImageRampups()) {
          // The image version which is marked as NEW can be picked for rampup up
          Optional<ImageVersion> imageVersion = imageVersionDao.getImageVersion(imageType.getName(),
              imageRampupRequest.getImageVersion(), State.NEW);
          if (imageVersion.isPresent()) {
            // During rampup plan creation all the verions will be marked as EXPERIMENTAL by
            // default in the image_rampup table. During the course of rampup, the version can be
            // marked as either STABLE or UNSTABLE using uddate API
            transOperator.update(INSERT_IMAGE_RAMPUP_QUERY, rampupPlanId,
                imageVersion.get().getId(), imageRampupRequest.getRampupPercentage(),
                StabilityTag.EXPERIMENTAL.getTagName(), imageType.getCreatedBy(), currentTimestamp,
                imageType.getModifiedBy(), currentTimestamp);
          } else {
            throw new ImageMgmtDaoException(String.format("Unable to get the image version: %s "
                    + "with version state: %s for image type: %s.  ",
                imageRampupRequest.getImageVersion(), State.NEW.getStateValue(),
                imageType.getName()));
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
      imageRampupPlanId = databaseOperator.transaction(insertAndGetRampupPlanId).intValue();
    } catch (SQLException e) {
      log.error("Unable to create the image version metadata", e);
      throw new ImageMgmtDaoException("Exception while creating image version metadata");
    }
    return imageRampupPlanId;
  }

  @Override
  public Optional<ImageRampupPlan> getActiveImageRampupPlan(String imageTypeName,
      boolean fetchRampup)
      throws ImageMgmtException {
    return fetchRampup ? getActiveImageRampupPlanAndRampup(imageTypeName) :
        getActiveImageRampupPlan(imageTypeName);
  }

  private Optional<ImageRampupPlan> getActiveImageRampupPlan(String imageTypeName)
      throws ImageMgmtException {
    try {
      return databaseOperator.query(SELECT_IMAGE_RAMPUP_ACTIVE_PLAN_QUERY,
          new FetchImageRampupPlanHandler(), imageTypeName, true);
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException("Exception while fetching image version");
    }
  }

  /**
   * Gets active rampup plan with rampup for the given image type
   *
   * @param imageTypeName
   * @return Optional<ImageRampupPlan>
   * @throws ImageMgmtException
   */
  private Optional<ImageRampupPlan> getActiveImageRampupPlanAndRampup(String imageTypeName)
      throws ImageMgmtException {
    try {
      Optional<ImageRampupPlan> imageRampupPlan = getActiveImageRampupPlan(imageTypeName);

      if (imageRampupPlan.isPresent()) {
        List<ImageRampup> imageRampups = databaseOperator.query(SELECT_IMAGE_RAMPUP_QUERY,
            new FetchImageRampupHandler(), imageRampupPlan.get().getId());
        imageRampupPlan.get().setImageRampups(imageRampups);
      }
      return imageRampupPlan;
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException("Exception while fetching image version");
    }
  }

  @Override
  public Map<String, List<ImageRampup>> fetchAllImageTypesRampup()
      throws ImageMgmtException {
    try {
      return databaseOperator.query(SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY,
          new FetchImageTypeRampupHandler(), true, true);
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException("Exception while fetching image version");
    }
  }

  @Override
  public Map<String, List<ImageRampup>> fetchRampupByImageTypes(Set<String> imageTypes)
      throws ImageMgmtException {
    try {
      if (imageTypes == null || imageTypes.isEmpty()) {
        return fetchAllImageTypesRampup();
      }
      StringBuilder queryBuilder = new StringBuilder(SELECT_ALL_IMAGE_TYPE_RAMPUP_QUERY);
      queryBuilder.append(" and ir.stability_tag in ( ?, ? ) ");
      queryBuilder.append(" and it.name in ( ");
      for (int i = 0; i < imageTypes.size() - 1; i++) {
        queryBuilder.append("? ,");
      }
      queryBuilder.append("? )");
      log.info("fetchRampupByImageTypes query: " + queryBuilder.toString());
      List<Object> params = new ArrayList<>();
      // Select only EXPERIMENTAL and STABLE versions that are being ramped up. Ignore the
      // UNSTABLE version.
      params.add(StabilityTag.EXPERIMENTAL.getTagName());
      params.add(StabilityTag.STABLE.getTagName());
      // Select active image rampup plan
      params.add(Boolean.TRUE);
      // Select active image type
      params.add(Boolean.TRUE);
      params.addAll(imageTypes);
      return databaseOperator.query(queryBuilder.toString(),
          new FetchImageTypeRampupHandler(), Iterables.toArray(params, Object.class));
    } catch (SQLException ex) {
      log.error("Exception while fetching image version ", ex);
      throw new ImageMgmtDaoException("Exception while fetching image version");
    }
  }

  @Override
  public void updateImageRampupPlan(ImageRampupPlanRequest imageRampupPlanRequest)
      throws ImageMgmtException {
    // Select the active rampup plan and the rampups for the given image type
    Optional<ImageRampupPlan> optionalImageRampupPlan =
        getActiveImageRampupPlanAndRampup(imageRampupPlanRequest.getImageTypeName());
    if (optionalImageRampupPlan.isPresent()) {
      ImageRampupPlan imageRampupPlan = optionalImageRampupPlan.get();
      /**
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
       *       "stability_tag": STABLE
       *     },
       *     {
       *       "imageVersion": "1.6.1",
       *       "rampupPercentage": "20",
       *       "stability_tag": STABLE
       *     }
       *   ]
       * }
       */
      // Set the ID of the plan to be updated.
      imageRampupPlanRequest.setId(imageRampupPlan.getId());
      /**
       * Update the internal ID (Key of image_rampup table) for each version present in the rampup
       * request so that update can be done using the ID. The below code will prepare version to
       * ID map based on the version and id available in the image_rampup table.
       */
      Map<String, Integer> versionIdKeyMap = new HashMap<>();
      for (ImageRampup imageRampup : imageRampupPlan.getImageRampups()) {
        versionIdKeyMap.put(imageRampup.getImageVersion(), imageRampup.getId());
      }

      // Use the version to id map created above to update the ID of each ramp up record.
      for (ImageRampupRequest imageRampupRequest : imageRampupPlanRequest
          .getImageRampups()) {
        if (versionIdKeyMap.containsKey(imageRampupRequest.getImageVersion())) {
          imageRampupRequest.setId(versionIdKeyMap.get(imageRampupRequest.getImageVersion()));
        } else {
          // Throw exception if invalid version is specified in the input request
          throw new ImageMgmtDaoException(String.format("Invalid version: %s specified in the "
              + "image rampup input.", imageRampupRequest.getImageVersion()));
        }
      }
      // Update image_rampup_plan and image_rampup table using the corresponding ID (key) and the
      // information to be updated.
      SQLTransaction<Integer> updateImageRamupTransaction = transOperator -> {
        // update image rampup plan
        updateImageRampupPlanInternal(imageRampupPlanRequest);
        // update each rampup
        for (ImageRampupRequest imageRampupRequest : imageRampupPlanRequest
            .getImageRampups()) {
          updateImageRampup(imageRampupRequest);
        }
        transOperator.getConnection().commit();
        return 1;
      };
      try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
        databaseOperator.transaction(updateImageRamupTransaction);
      } catch (SQLException e) {
        log.error("Unable to create the image version metadata", e);
        throw new ImageMgmtDaoException("Exception while creating image version metadata");
      }
    } else {
      throw new ImageMgmtDaoException(
          String.format("There is no active rampup plan found for image "
              + "type: %s.", imageRampupPlanRequest.getImageTypeName()));
    }
  }

  /**
   * This method updates image rampup plan for an image type and the plan can be either made active
   * or inactive.
   *
   * @param imageRampupPlanRequest
   * @throws ImageMgmtException
   */
  private void updateImageRampupPlanInternal(ImageRampupPlanRequest imageRampupPlanRequest)
      throws ImageMgmtException {
    try {
      List<Object> params = new ArrayList<>();
      StringBuilder queryBuilder = new StringBuilder("update image_rampup_plan set ");
      // As update is allowed only for active plan, if activatePlan is false then deactivate the
      // current plan.
      if (!imageRampupPlanRequest.isActivatePlan()) {
        queryBuilder.append(" active = ?, ");
        params.add(Boolean.valueOf(imageRampupPlanRequest.isActivatePlan()));
      }
      queryBuilder.append(" modified_by = ?, modified_on = ?");
      params.add(imageRampupPlanRequest.getModifiedBy());
      params.add(Timestamp.valueOf(LocalDateTime.now()));
      queryBuilder.append(" where id = ? ");
      params.add(imageRampupPlanRequest.getId());
      databaseOperator.update(queryBuilder.toString(), Iterables.toArray(params, Object.class));
    } catch (SQLException ex) {
      log.error("Exception while updating image version ", ex);
      throw new ImageMgmtDaoException("Exception while updating image version");
    }
  }

  /**
   * This method up updates image rampup for an image type. The information such as version rampup
   * percentage, stability tag etc. can be updated.
   *
   * @param imageRampupRequest
   * @throws ImageMgmtException
   */
  private void updateImageRampup(ImageRampupRequest imageRampupRequest)
      throws ImageMgmtException {
    try {
      List<Object> params = new ArrayList<>();
      StringBuilder queryBuilder = new StringBuilder("update image_rampup set ");
      if (imageRampupRequest.getRampupPercentage() != null) {
        queryBuilder.append(" rampup_percentage = ?, ");
        params.add(imageRampupRequest.getRampupPercentage());
      }
      if (imageRampupRequest.getStabilityTag() != null) {
        queryBuilder.append(" stability_tag = ?, ");
        params.add(imageRampupRequest.getStabilityTag().getTagName());
      }
      queryBuilder.append(" modified_by = ?, modified_on = ?");
      params.add(imageRampupRequest.getModifiedBy());
      params.add(Timestamp.valueOf(LocalDateTime.now()));
      queryBuilder.append(" where id = ? ");
      params.add(imageRampupRequest.getId());
      databaseOperator.update(queryBuilder.toString(), Iterables.toArray(params, Object.class));
    } catch (SQLException ex) {
      log.error("Exception while updating image version ", ex);
      throw new ImageMgmtDaoException("Exception while updating image version");
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image rampup plan
   */
  public static class FetchImageRampupPlanHandler implements
      ResultSetHandler<Optional<ImageRampupPlan>> {

    @Override
    public Optional<ImageRampupPlan> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Optional.empty();
      }
      ImageRampupPlan imageRampupPlan = new ImageRampupPlan();
      do {
        int id = rs.getInt("id");
        String name = rs.getString("name");
        String description = rs.getString("description");
        String imageTypeName = rs.getString("image_type_name");
        boolean active = rs.getBoolean("active");
        String createdOn = rs.getString("created_on");
        String createdBy = rs.getString("created_by");
        String modifiedOn = rs.getString("modified_on");
        String modifiedBy = rs.getString("modified_by");
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
    public List<ImageRampup> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      List<ImageRampup> imageRampups = new ArrayList<>();
      do {
        int id = rs.getInt("id");
        int planId = rs.getInt("plan_id");
        String imageVersion = rs.getString("version");
        int rampupPercentage = rs.getInt("rampup_percentage");
        String stabilityTag = rs.getString("stability_tag");
        String createdOn = rs.getString("created_on");
        String createdBy = rs.getString("created_by");
        String modifiedOn = rs.getString("modified_on");
        String modifiedBy = rs.getString("modified_by");
        ImageRampup imageRampup = new ImageRampup();
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
      log.info("imageRampups:" + imageRampups);
      return imageRampups;
    }
  }

  /**
   * ResultSetHandler implementation class for fetching image rampup details for given image types
   */
  public static class FetchImageTypeRampupHandler implements
      ResultSetHandler<Map<String, List<ImageRampup>>> {

    @Override
    public Map<String, List<ImageRampup>> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }
      Map<String, List<ImageRampup>> imageRampupMap = new LinkedHashMap<>();
      do {
        int id = rs.getInt("id");
        int planId = rs.getInt("plan_id");
        String imageTypeName = rs.getString("image_type_name");
        String imageVersion = rs.getString("image_version");
        int rampupPercentage = rs.getInt("rampup_percentage");
        String stabilityTag = rs.getString("stability_tag");
        String createdOn = rs.getString("created_on");
        String createdBy = rs.getString("created_by");
        String modifiedOn = rs.getString("modified_on");
        String modifiedBy = rs.getString("modified_by");
        ImageRampup imageRampup = new ImageRampup();
        imageRampup.setId(id);
        imageRampup.setImageVersion(imageVersion);
        imageRampup.setRampupPercentage(rampupPercentage);
        //imageRampupVersion.setStabilityTag(StabilityTag.fromTagName(stabilityTag));
        imageRampup.setCreatedOn(createdOn);
        imageRampup.setCreatedBy(createdBy);
        imageRampup.setModifiedBy(modifiedBy);
        imageRampup.setModifiedOn(modifiedOn);
        if (imageRampupMap.containsKey(imageTypeName)) {
          imageRampupMap.get(imageTypeName).add(imageRampup);
        } else {
          List<ImageRampup> imageRampupList = new ArrayList<>();
          imageRampupList.add(imageRampup);
          imageRampupMap.put(imageTypeName, imageRampupList);
        }
      } while (rs.next());
      return imageRampupMap;
    }
  }
}
