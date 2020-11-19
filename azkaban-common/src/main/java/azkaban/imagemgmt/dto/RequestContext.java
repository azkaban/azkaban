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
package azkaban.imagemgmt.dto;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a data transfer object (DTO) to propagate image management REST API json
 * payload, query params, pagination params etc. This class can be used to pass these information
 * to the different layers of the API implementation.
 */
public class RequestContext {
  // Represents json payload in string format. Json payload is provided as part of API invocation
  private String jsonPayload;
  // The user who invoked the REST API
  private String user;
  // Map containing query parameters
  private Map<String, Object> params;
  // pagination parameters
  private int start;
  private int limit;

  private RequestContext(Builder builder) {
    this.jsonPayload = builder.jsonPayload;
    this.user = builder.user;
    this.params = builder.params;
    this.start = builder.start;
    this.limit = builder.limit;
  }

  public String getJsonPayload() {
    return jsonPayload;
  }

  public String getUser() {
    return user;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for building RequestContext
   */
  public static class Builder {
    private String jsonPayload;
    private String user;
    private Map<String, Object> params;
    private int start;
    private int limit;

    public Builder() {
      this.params = new HashMap<>();
    }

    /**
     * Sets the json payload provided as part of image management REST API invocation
     * @param jsonPayload input json payload in string format
     * @return Builder - returns builder instance
     * @throws NullPointerException if json payload is missing
     */
    public Builder jsonPayload(String jsonPayload) {
      Preconditions.checkNotNull(jsonPayload);
      this.jsonPayload = jsonPayload;
      return this;
    }

    /**
     * Sets the user who invokes the image management REST API.
     * @param user - user in string
     * @return Builder - returns builder instance
     * @throws NullPointerException if user is null
     */
    public Builder user(String user) {
      Preconditions.checkNotNull(user);
      this.user = user;
      return this;
    }

    /**
     * Adds param to the param map. It accepts non null key and non null value. This method must be
     * user for mandatory parameters
     * @param paramKey - key of the parameter
     * @param paramValue - value of the parameter
     * @return Builder - returns builder instance
     * @throws NullPointerException if preconditions failed
     */
    public Builder addParam(String paramKey, Object paramValue) {
      // Param key must not be null
      Preconditions.checkNotNull(paramKey, "Param key is null");
      // Param value must not be null
      Preconditions.checkNotNull(paramValue, "The mandatory parameter "+paramKey+" is "
          + "either missing or contains null value.");
      this.params.put(paramKey, paramValue);
      return this;
    }

    /**
     * Adds param to the param map. It accepts non null key. The value can be null. This method
     * must be used for optional parameters
     * @param paramKey - key of the parameter
     * @param paramValue - value of the parameter
     * @return Builder - returns builder instance
     * @throws NullPointerException if preconditions failed
     */
    public Builder addParamIfPresent(String paramKey, Object paramValue) {
      // Both param key and value must be present and should not be null
      Preconditions.checkNotNull(paramKey, "Param key is null");
      // Ignore the key value if not specified
      if(paramValue!=null) {
        this.params.put(paramKey, paramValue);
      }
      return this;
    }

    /**
     * Sets the start offset for pagination
     * @param start - start offset for pagination
     * @return Builder - returns builder instance
     * @throws IllegalArgumentException if precondition is not satisfied
     */
    public Builder start(int start) {
      Preconditions.checkArgument(start > 0, "Pagination start offset must be positive");
      this.start = start;
      return this;
    }

    /**
     * Sets the limit for pagination i.e. items to fetch in one request
     * @param limit - limit for pagination.
     * @return Builder - returns builder instance
     * @throws IllegalArgumentException if precondition is not satisfied
     */
    public Builder limit(int limit) {
      Preconditions.checkArgument(limit > 0, "Pagination Limit must be positive");
      this.limit = limit;
      return this;
    }

    /**
     * Invoke build method to build the final RequestContext
     * @return RequestContext
     */
    public RequestContext build() {
      return new RequestContext(this);
    }
  }
}
