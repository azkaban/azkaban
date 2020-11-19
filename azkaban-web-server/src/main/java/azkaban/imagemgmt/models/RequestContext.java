package azkaban.imagemgmt.models;

import java.util.HashMap;
import java.util.Map;

public class RequestContext {
  private String jsonPayload;
  private String user;
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

  public static class Builder {
    private String jsonPayload;
    private String user;
    private Map<String, Object> params;
    private int start;
    private int limit;

    public Builder() {
      this.params = new HashMap<>();
    }

    public Builder jsonPayload(String jsonPayload) {
      this.jsonPayload = jsonPayload;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder addParam(String paramKey, Object paramValue) {
      if(paramKey!=null) {
        this.params.put(paramKey, paramValue);
      }
      return this;
    }

    public Builder addNonNullParam(String paramKey, Object paramValue) {
      if(paramKey != null && paramValue != null) {
        this.params.put(paramKey, paramValue);
      }
      return this;
    }

    public Builder start(int start) {
      this.start = start;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public RequestContext build() {
      return new RequestContext(this);
    }
  }
}
