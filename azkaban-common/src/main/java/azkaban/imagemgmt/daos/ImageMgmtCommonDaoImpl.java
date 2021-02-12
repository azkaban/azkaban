/*
 * Copyright 2021 LinkedIn Corp.
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
import azkaban.imagemgmt.dto.DeleteResponse;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.models.ImageVersionUsageData;
import com.google.common.collect.Iterables;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for image management common DAO. Provides implementation for all the common
 * functionalities for image management. Once of the such common functionality is delete image
 * version and its associated metadata.
 */
public class ImageMgmtCommonDaoImpl implements ImageMgmtCommonDao {

  private static final Logger log = LoggerFactory.getLogger(ImageRampupDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  private final ImageTypeDao imageTypeDao;

  private final ImageVersionDao imageVersionDao;

  private final ImageRampupDao imageRampupDao;

  // Make sure correct rampups are deleted based on the image type and version id
  private static final String DELETE_IMAGE_RAMPUP_BY_VERSION_ID_QUERY = "delete from image_rampup "
      + "where plan_id in ( ${plan_ids} )";

  // Make sure correct rampup plan is deleted for the specified image type
  private static final String DELETE_IMAGE_RAMPUP_PLAN_BY_PLAN_ID_QUERY = "delete from "
      + "image_rampup_plan where id in ( ${plan_ids} )";

  // Make sure correct image version is deleted based on image type and version id
  private static final String DELETE_IMAGE_VERSION_BY_IMAGE_TYPE_AND_VERSION_ID_QUERY = "delete "
      + "iv from image_versions iv inner join image_types it on it.id = iv.type_id where iv.id = "
      + "? and  lower(it.name) = ? ";

  @Inject
  public ImageMgmtCommonDaoImpl(final DatabaseOperator databaseOperator,
      final ImageTypeDao imageTypeDao,
      final ImageVersionDao imageVersionDao,
      final ImageRampupDao imageRampupDao) {
    this.databaseOperator = databaseOperator;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
    this.imageRampupDao = imageRampupDao;
  }

  @Override
  public DeleteResponse deleteImageVersion(final String imageTypeName, final int versionId,
      final Boolean forceDelete) throws ImageMgmtException {
    DeleteResponse deleteResponse = new DeleteResponse();
    // Get the image version for the image type and id. Throw exception if not exits.
    Optional<ImageVersion> imageVersion = this.imageVersionDao.getImageVersion(
        imageTypeName, versionId);
    if(!imageVersion.isPresent()) {
      deleteResponse.setErrorCode(ErrorCode.NOT_FOUND);
      deleteResponse.setMessage(String.format("The version id: %d for image type: %s does not "
              + "exist.", versionId, imageTypeName));
      return deleteResponse;
    }
    // Get the rampup plan and rampup details for the image type and version id.
    List<ImageRampupPlan> imageRampupPlans = this.imageRampupDao.getImageRampupPlans(imageTypeName,
        versionId);
    // Collect the plan ids and delete the image rampup and rampup plan using that.
    List<Integer> planIds = new ArrayList<>();
    if(!CollectionUtils.isEmpty(imageRampupPlans)) {
      for(ImageRampupPlan imageRampupPlan : imageRampupPlans) {
        planIds.add(imageRampupPlan.getId());
      }
      // If the version is used in any of the rampup plans throw error with the rampup plan details.
      if(!forceDelete) {
        log.info(String.format("The version id: %d (i.e. version: %s) for image type: %s is part "
            + "of the existing rampup plans %s", versionId, imageVersion.get().getVersion(),
            imageTypeName, imageRampupPlans));
        ImageVersionUsageData imageVersionUsageData = new ImageVersionUsageData(imageVersion.get(),
            imageRampupPlans);
        deleteResponse.setData(imageVersionUsageData);
        deleteResponse.setErrorCode(ErrorCode.BAD_REQUEST);
        deleteResponse.setMessage(String.format("The version id: %d (i.e. version: %s) for "
            + "image type: %s has below state and is associated with below rampup plans. Still "
                + "want to proceed with the delete specify 'forceDelete' parameter as true.",
            versionId, imageVersion.get().getVersion(), imageTypeName));
        return deleteResponse;
      }
    } else {
      if(!forceDelete && (State.ACTIVE.equals(imageVersion.get().getState()) ||
          State.NEW.equals(imageVersion.get().getState()))) {
        ImageVersionUsageData imageVersionUsageData = new ImageVersionUsageData(imageVersion.get(),
            null);
        deleteResponse.setData(imageVersionUsageData);
        deleteResponse.setErrorCode(ErrorCode.BAD_REQUEST);
        deleteResponse.setMessage(String.format("The version id: %d (i.e. version: %s) for "
                + "image type: %s is ACTIVE/NEW. Still want to "
                + "proceed with the delete specify 'forceDelete' parameter as true.", versionId,
            imageVersion.get().getVersion(), imageTypeName));
        return deleteResponse;
      }
    }
    // If forceDelete is set to true delete all the metadata pertaining the the image version
    // including rampup plan and rampup details (if exists).
    this.deleteImageVersionAndRampupMetadata(imageTypeName,versionId, planIds);
    deleteResponse.setMessage(String.format("Successfully deleted image version metadata for "
        + "version id: %d (i.e. version: %s) for image type: %s", versionId,
        imageVersion.get().getVersion(), imageTypeName));
    return deleteResponse;
  }

  private void deleteImageVersionAndRampupMetadata(final String imageTypeName, final int versionId,
      List<Integer> planIds) throws ImageMgmtException {
    // If there is no plan delete the corresponding image version.
    if(CollectionUtils.isEmpty(planIds)) {
      this.deleteImageVersion(imageTypeName, versionId);
      return;
    }
    // In clause builder
    final StringBuilder inClauseBuilder = new StringBuilder();
    for (int i = 0; i < planIds.size(); i++) {
      inClauseBuilder.append("?,");
    }
    inClauseBuilder.setLength(inClauseBuilder.length() - 1);
    final Map<String, String> valueMap = new HashMap<>();
    valueMap.put("plan_ids", inClauseBuilder.toString());
    final StrSubstitutor strSubstitutor = new StrSubstitutor(valueMap);
    final String imageRampupDeleteQuery =
        strSubstitutor.replace(DELETE_IMAGE_RAMPUP_BY_VERSION_ID_QUERY);
    final String imageRampupPlanDeleteQuery =
        strSubstitutor.replace(DELETE_IMAGE_RAMPUP_PLAN_BY_PLAN_ID_QUERY);
    final SQLTransaction<Integer> deleteImageVersionMetadata = transOperator -> {
      // Passing timestamp from the code base and can be formatted accordingly based on timezone
      final Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
      // delete all the rampups containing the image version for the image type
      transOperator.update(imageRampupDeleteQuery, Iterables.toArray(planIds, Object.class));
      // delete all the rampups plans containing the image version for the image type
      transOperator.update(imageRampupPlanDeleteQuery, Iterables.toArray(planIds, Object.class));
      // delete the image version for the given id and image type
      transOperator.update(DELETE_IMAGE_VERSION_BY_IMAGE_TYPE_AND_VERSION_ID_QUERY, versionId,
          imageTypeName.toLowerCase());
      transOperator.getConnection().commit();
      return 1;
    };

    int deleteCount = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      deleteCount = this.databaseOperator.transaction(deleteImageVersionMetadata).intValue();
      if (deleteCount < 1) {
        log.error(String.format("Exception while deleting from image version due to invalid input, "
            + "deleteCount: %d.", deleteCount));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while deleting from image "
            + "version due to invalid input");
      }
    } catch (final SQLException e) {
      log.error("Unable to delete image version metadata", e);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while deleting image "
          + "version metadata. " + e.getMessage());
    }
  }

  private void deleteImageVersion(final String imageTypeName, final int versionId)
      throws ImageMgmtException {
    final SQLTransaction<Integer> deleteImageVersionMetadata = transOperator -> {
      // delete the image version for the given id and image type
      transOperator.update(DELETE_IMAGE_VERSION_BY_IMAGE_TYPE_AND_VERSION_ID_QUERY, versionId,
          imageTypeName);
      transOperator.getConnection().commit();
      return 1;
    };

    int deleteCount = 0;
    try {
      /* what will happen if there is a partial failure in
         any of the below statements?
         Ideally all should happen in a transaction */
      deleteCount = this.databaseOperator.transaction(deleteImageVersionMetadata).intValue();
      if (deleteCount < 1) {
        log.error(String.format("Exception while deleting from image version due to invalid input, "
            + "deleteCount: %d.", deleteCount));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while deleting from image "
            + "version due to invalid input");
      }
    } catch (final SQLException e) {
      log.error("Unable to delete image version metadata", e);
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while deleting image "
          + "version metadata. " + e.getMessage());
    }
  }
}
