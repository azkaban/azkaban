package azkaban.utils;

import java.net.URI;
import java.util.Map;

import org.apache.log4j.Logger;
import org.mortbay.util.ajax.JSON;

import azkaban.executor.ExecutorApiClient;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;

/**
 * Helper class wrapping REST API client for Nyx Service
 * 
 * @author gaggarwa
 *
 */
public class NyxUtils {
  private static Logger logger = Logger.getLogger(NyxUtils.class);
  public static final String NYX_SERVER_PORT = "nyx.service.port";
  public static final String NYX_SERVER_HOST = "nyx.service.host";

  private static String nyxServiceHost = "loclahost";
  private static final boolean isHttp = true;
  private static int port = 8080;

  static {
    // populating nyx service from .properties configs
    Props props = TriggerManager.getAzprops();
    if (props != null) {
      nyxServiceHost = props.getString(NYX_SERVER_HOST, nyxServiceHost);
      port = props.getInt(NYX_SERVER_PORT, port);
    }
  }

  /**
   * Use trigger json specification to register a trigger with Nyx Service
   * 
   * @throws TriggerManagerException
   */
  public static long registerNyxTrigger(String specificationJson)
      throws TriggerManagerException {
    try {
      ExecutorApiClient client = ExecutorApiClient.getInstance();
      URI uri = client.buildUri(nyxServiceHost, port, "/register", isHttp);
      String rawResponse = client.httpPost(uri, null, specificationJson);
      Map<String, Object> parsedResponse =
          (Map<String, Object>) JSON.parse(rawResponse);

      // TODO: to be revisited. Presence of an "id" field signify successfully
      // registration of trigger
      if (parsedResponse.containsKey("error")) {
        throw new IllegalArgumentException(
            (String) parsedResponse.get("error"));
      } else if (parsedResponse.containsKey("id")) {
        return Long.parseLong((String) parsedResponse.get("id"));
      } else {
        throw new Exception("Failed to parse Nyx response " + rawResponse);
      }
    } catch (Exception ex) {
      logger.error(
          "Failed to get Nyx service response for :" + specificationJson, ex);
      throw new TriggerManagerException(ex);
    }
  }

  /**
   * Delete an already registered trigger from Nyx Service
   * 
   * @throws TriggerManagerException
   */
  public static void unregisterNyxTrigger(Long triggerId)
      throws TriggerManagerException {
    if (triggerId == -1) {
      throw new TriggerManagerException("Trigger is not registered");
    }

    try {
      ExecutorApiClient client = ExecutorApiClient.getInstance();
      URI uri = client.buildUri(nyxServiceHost, port,
          "/unregister/" + triggerId, isHttp);
      String response = client.httpGet(uri, null);
      Map<String, Object> parsedResponse =
          (Map<String, Object>) JSON.parse(response);

      if (parsedResponse.containsKey("error")) {
        throw new Exception((String) parsedResponse.get("error"));
      }
    } catch (Exception ex) {
      logger.error(
          "Failed to get Nyx service response for triggerId : " + triggerId,
          ex);
      throw new TriggerManagerException(ex);
    }
  }

  /**
   * Look up status of an already registered trigger
   * 
   * @param triggerId
   * @return status fetched from Nyx
   * @throws TriggerManagerException
   */
  public static boolean isNyxTriggerReady(Long triggerId)
      throws TriggerManagerException {
    if (triggerId == -1) {
      throw new TriggerManagerException("Trigger is not registered");
    }

    try {
      ExecutorApiClient client = ExecutorApiClient.getInstance();
      URI uri =
          client.buildUri(nyxServiceHost, port, "/status/" + triggerId, isHttp);
      String response = client.httpGet(uri, null);
      Map<String, Object> parsedResponse =
          (Map<String, Object>) JSON.parse(response);

      if (parsedResponse.containsKey("error")) {
        throw new IllegalArgumentException(
            (String) parsedResponse.get("error"));
      } else if (parsedResponse.containsKey("ready")) {
        return (boolean) parsedResponse.get("ready");
      } else {
        throw new Exception("Status missing from Nyx response :" + response);
      }
    } catch (Exception ex) {
      logger.error(
          "Failed to get Nyx service response for triggerId : " + triggerId,
          ex);
      throw new TriggerManagerException(ex);
    }
  }
}
