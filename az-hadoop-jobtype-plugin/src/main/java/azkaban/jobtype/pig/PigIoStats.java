/*
 * Copyright 2012 LinkedIn Corp.
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
package azkaban.jobtype.pig;

import java.util.HashMap;
import java.util.Map;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;


public class PigIoStats {

  private long bytes;
  private long records;
  private String location;
  private String name;

  public PigIoStats(OutputStats stats) {
    this.bytes = stats.getBytes();
    this.records = stats.getNumberRecords();
    this.name = stats.getName();
    this.location = stats.getLocation();
  }

  public PigIoStats(InputStats stats) {
    this.bytes = stats.getBytes();
    this.records = stats.getNumberRecords();
    this.name = stats.getName();
    this.location = stats.getLocation();
  }

  public PigIoStats(String name, String location, long bytes, long records) {
    this.bytes = bytes;
    this.records = records;
    this.name = name;
    this.location = location;
  }

  public long getBytes() {
    return this.bytes;
  }

  public long getNumberRecords() {
    return this.records;
  }

  public String getLocation() {
    return this.location;
  }

  public String getName() {
    return this.name;
  }

  public Object toJson() {
    Map<String, String> jsonObj = new HashMap<String, String>();
    jsonObj.put("bytes", Long.toString(bytes));
    jsonObj.put("location", location);
    jsonObj.put("name", name);
    jsonObj.put("numberRecords", Long.toString(records));
    return jsonObj;
  }

  public static PigIoStats fromJson(Object obj) {
    @SuppressWarnings("unchecked")
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;

    String name = (String) jsonObj.get("name");
    long bytes = Long.parseLong((String) jsonObj.get("bytes"));
    long records = Long.parseLong((String) jsonObj.get("numberRecords"));
    String location = (String) jsonObj.get("location");
    return new PigIoStats(name, location, bytes, records);
  }
}
