package azkaban.project;

import azkaban.utils.Props;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatasetUtils {

  private static ObjectMapper mapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(DatasetUtils.class);
  private static final String PATH_DELIMTER = "/";

  /**
   * Reads a list of hashmap objects containing dataset fields as key and convert it to a list of dataset objects
   * @param datasetMap list of map containing dataset fields as key
   * @return list of Dataset
   */
  public static List<Dataset> convertMapToDataset(final List<HashMap<String, String>> datasetMap) {
    List<Dataset> outputDataset = Optional.ofNullable(datasetMap).orElse(Collections.emptyList()).stream()
        .map(dataset -> mapper.convertValue(dataset, Dataset.class))
        .collect(Collectors.toList());
    return outputDataset;
  }

  /**
   * Converts the list of dataset objects into a json string
   * @param datasetList list of dataset objects
   * @return dataset objects as string
   */
  public static String datasetListToPropJsonString(List<Dataset> datasetList) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(datasetList);
  }

  /**
   * Converts a dataset object into a string separated by delimiter
   * @param dataset
   * @return a delimiter separated dataset object as string
   */
  private static String datasetToPropString(Dataset dataset) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(dataset);
  }

  /**
   * Replace all job props name specified in the dataset path with their property value from props object
   * @param datasetList list of dataset object
   * @param props contains all job properties
   * @return the dataset list with dataset path replaced with the property value from props object
   */
  public static List<Dataset> replaceDatasetPathWithPropValue(List<Dataset> datasetList, final Props props) {
    for(Dataset dataset: datasetList) {
      String updatedDatasetPath = replaceDatasetPathValue(dataset.getPath(), props);
      dataset.setPath(updatedDatasetPath);
    }
    return datasetList;
  }

  /**
   * Replaces the string containing azkaban prop with template ${PropName} with actual value from props object
   * Sample input: /jobs/${user.to.proxy}/${azkaban.flow.flowid}/${azkaban.flow.execid}/dataset_1
   * Sample output: /jobs/foo/test/1/dataset_1
   * @param datasetPath string containing the azkaban prop name
   * @param props contains all job properties
   * @return a string with prop values for azkaban property name in the input string
   */
  private static String replaceDatasetPathValue(String datasetPath, Props props) {
    List<String> datasetPathAsList = Arrays.asList(datasetPath.split(PATH_DELIMTER));
    Pattern pattern = Pattern.compile("\\$\\{.*\\}");

    List<String> updatedDatasetPathAsList = new ArrayList<>();
    for (String dirName : datasetPathAsList) {
      Matcher matcher = pattern.matcher(dirName);
      if (matcher.matches()) {
        final int propNameStartIndex = 2; // actual property name starts from index 2, ${PropName}
        final int propNameEndIndex = dirName.length() - 1;
        String propName = dirName.substring(propNameStartIndex, propNameEndIndex);
        if (props.containsKey(propName)) {
          String propValue = props.getString(propName);
          updatedDatasetPathAsList.add(propValue);
        } else {
          throw new RuntimeException(String.format("Property %s is not available in Azkaban job props", propName));
        }
      } else {
        updatedDatasetPathAsList.add(dirName);
      }
    }
    return String.join(PATH_DELIMTER, updatedDatasetPathAsList);
  }

  /**
   * Converts the json string to a list of dataset objects
   * Sample input string:
   * [{"type":"HDFS_DYNAMIC_PATH","name":"output_ds_1_0","path":"/jobs/azkaban/spark_flow/120/output_dataset_1_0"},
   * {"type":"HDFS_DYNAMIC_PATH","name":"output_ds_1_1","path":"/jobs/azkaban/spark_flow/120/output_dataset_1_1"}]
   * @param datasetStr input string for converting to dataset list
   * @return list of dataset objects
   */
  public static List<Dataset> datasetStringToObject(final String datasetStr) {
    try {
      List<Dataset> list = mapper.readValue(datasetStr, new TypeReference<List<Dataset>>() {});
      return list;
    } catch (Exception e) {
      final String errorMsg = String.format("Exception while parsing dataset string: %s", datasetStr);
      logger.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /**
   * Converts the json string to the dataset object
   * Sample input string:
   * {"type":"HDFS_DYNAMIC_PATH","name":"input_ds","path":"/jobs/azkaban/spark_flow/120/dataset_1",
   * "annotation":{"k1":"v1","k2":"v2"}}
   * @param datasetStr
   * @return
   */
  public static Dataset datasetJsonStringToObject(final String datasetStr) {
    try {
      Dataset list = mapper.readValue(datasetStr, Dataset.class);
      return list;
    } catch (Exception e) {
      final String errorMsg = String.format("Exception while parsing dataset string: %s", datasetStr);
      logger.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /**
   * Replace all job props name specified in the dataset path with their actual value from props object
   * returns a map having dataset object with raw path as key and resolved path as value
   * Sample entry in currentExecOutputDatasetMap:
   * /jobs/${user.to.proxy}/${azkaban.flow.flowid}/${azkaban.flow.execid}/dataset_1 -> /jobs/foo/test/1/dataset_1
   * Sample output map entry:
   * HDFS_DYNAMIC_PATH:ds_1:/jobs/${user.to.proxy}/${azkaban.flow.flowid}/${azkaban.flow.execid}/dataset_1 ->
   *   HDFS_DYNAMIC_PATH:ds_1:/jobs/foo/test/1/dataset_1
   * @param datasetList
   * @param currentExecOutputDatasetMap map having raw dataset path as key and resolved dataset path as value
   * @return a map having dataset object with raw path as key and resolved path as value
   */
  public static Map<String, String> resolveInputDatasetAndCreateRawToResolvedDatasetMap(final List<Dataset> datasetList,
      final Map<String, String> currentExecOutputDatasetMap, final Props props) throws IOException {
    Map<String, String> rawToResolvedDatasetMap = new HashMap<>();
    for (Dataset dataset : datasetList) {
      String rawDataset = DatasetUtils.datasetToPropString(dataset);
      String rawDatasetPath = dataset.getPath();
      if (MapUtils.isNotEmpty(currentExecOutputDatasetMap) &&
          currentExecOutputDatasetMap.containsKey(rawDatasetPath)) {
        dataset.setPath(currentExecOutputDatasetMap.get(rawDatasetPath));
      } else {
        dataset.setPath(replaceDatasetPathValue(rawDatasetPath, props));
      }
      String resolvedDataset = DatasetUtils.datasetToPropString(dataset);
      rawToResolvedDatasetMap.put(rawDataset, resolvedDataset);
    }
    return rawToResolvedDatasetMap;
  }

  /**
   * Creates a map for a list of dataset objects with raw dataset path having raw path as key and resolved path as value
   * Sample output map entry:
   * HDFS_DYNAMIC_PATH:ds_1:/jobs/${user.to.proxy}/${azkaban.flow.flowid}/${azkaban.flow.execid}/dataset_1 ->
   *  HDFS_DYNAMIC_PATH:ds_1:/jobs/foo/test/1/dataset_1
   * @param datasetList
   * @param props contains all job properties
   * @return a map having dataset object having raw path as key and resolved path as value
   */
  public static Map<String, String> resolveOutputDatasetAndCreateRawToResolvedDatasetMap(
      final List<Dataset> datasetList, final Props props) throws IOException {
    Map<String, String> rawToResolvedDatasetMap = new HashMap<>();
    for(Dataset dataset: datasetList) {
      String key = datasetToPropString(dataset);
      String resolvedPath = replaceDatasetPathValue(dataset.getPath(), props);
      dataset.setPath(resolvedPath);
      String value = datasetToPropString(dataset);
      rawToResolvedDatasetMap.put(key, value);
    }
    return rawToResolvedDatasetMap;
  }

}
