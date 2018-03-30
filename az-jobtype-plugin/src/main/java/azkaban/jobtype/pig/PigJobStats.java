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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;

public class PigJobStats {
  private int numberMaps;
  private int numberReduces;

  private long minMapTime;
  private long maxMapTime;
  private long avgMapTime;

  private long minReduceTime;
  private long maxReduceTime;
  private long avgReduceTime;

  private long bytesWritten;
  private long hdfsBytesWritten;

  private long mapInputRecords;
  private long mapOutputRecords;
  private long reduceInputRecords;
  private long reduceOutputRecords;

  private long proactiveSpillCountObjects;
  private long proactiveSpillCountRecs;

  private long recordsWritten;
  private long smmSpillCount;

  private String errorMessage;

  private List<PigIoStats> inputStats;
  private List<PigIoStats> outputStats;

  public PigJobStats() {
  }

  public PigJobStats(
      int numberMaps,
      int numberReduces,
      long minMapTime,
      long maxMapTime,
      long avgMapTime,
      long minReduceTime,
      long maxReduceTime,
      long avgReduceTime,
      long bytesWritten,
      long hdfsBytesWritten,
      long mapInputRecords,
      long mapOutputRecords,
      long reduceInputRecords,
      long reduceOutputRecords,
      long proactiveSpillCountObjects,
      long proactiveSpillCountRecs,
      long recordsWritten,
      long smmSpillCount,
      String errorMessage,
      List<PigIoStats> inputStats,
      List<PigIoStats> outputStats) {
    this.numberMaps = numberMaps;
    this.numberReduces = numberReduces;

    this.minMapTime = minMapTime;
    this.maxMapTime = maxMapTime;
    this.avgMapTime = avgMapTime;

    this.minReduceTime = minReduceTime;
    this.maxReduceTime = maxReduceTime;
    this.avgReduceTime = avgReduceTime;

    this.bytesWritten = bytesWritten;
    this.hdfsBytesWritten = hdfsBytesWritten;

    this.mapInputRecords = mapInputRecords;
    this.mapOutputRecords = mapOutputRecords;
    this.reduceInputRecords = reduceInputRecords;
    this.reduceOutputRecords = reduceOutputRecords;

    this.proactiveSpillCountObjects = proactiveSpillCountObjects;
    this.proactiveSpillCountRecs = proactiveSpillCountRecs;

    this.recordsWritten = recordsWritten;
    this.smmSpillCount = smmSpillCount;

    this.errorMessage = errorMessage;
    this.inputStats = inputStats;
    this.outputStats = outputStats;
  }

  public PigJobStats(JobStats stats) {
    numberMaps = stats.getNumberMaps();
    minMapTime = stats.getMinMapTime();
    maxMapTime = stats.getMaxMapTime();
    avgMapTime = stats.getAvgMapTime();

    numberReduces = stats.getNumberReduces();
    minReduceTime = stats.getMinReduceTime();
    maxReduceTime = stats.getMaxReduceTime();
    avgReduceTime = stats.getAvgREduceTime();

    bytesWritten = stats.getBytesWritten();
    hdfsBytesWritten = stats.getHdfsBytesWritten();

    mapInputRecords = stats.getMapInputRecords();
    mapOutputRecords = stats.getMapOutputRecords();
    reduceInputRecords = stats.getReduceInputRecords();
    reduceOutputRecords = stats.getReduceOutputRecords();

    proactiveSpillCountObjects = stats.getProactiveSpillCountObjects();
    proactiveSpillCountRecs = stats.getProactiveSpillCountRecs();

    recordsWritten = stats.getRecordWrittern();
    smmSpillCount = stats.getSMMSpillCount();

    errorMessage = stats.getErrorMessage();

    List<InputStats> inputs = stats.getInputs();
    inputStats = new ArrayList<PigIoStats>();
    for (InputStats input : inputs) {
      inputStats.add(new PigIoStats(input.getName(), input.getLocation(), input
          .getBytes(), input.getNumberRecords()));
    }

    List<OutputStats> outputs = stats.getOutputs();
    outputStats = new ArrayList<PigIoStats>();
    for (OutputStats output : outputs) {
      outputStats.add(new PigIoStats(output.getName(), output.getLocation(),
          output.getBytes(), output.getNumberRecords()));
    }
  }

  public int getNumberMaps() {
    return numberMaps;
  }

  public int getNumberReduces() {
    return numberReduces;
  }

  public long getMinMapTime() {
    return minMapTime;
  }

  public long getMaxMapTime() {
    return maxMapTime;
  }

  public long getAvgMapTime() {
    return avgMapTime;
  }

  public long getMinReduceTime() {
    return minReduceTime;
  }

  public long getMaxReduceTime() {
    return maxReduceTime;
  }

  public long getAvgReduceTime() {
    return avgReduceTime;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  public long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }

  public long getMapInputRecords() {
    return mapInputRecords;
  }

  public long getMapOutputRecords() {
    return mapOutputRecords;
  }

  public long getReduceInputRecords() {
    return reduceInputRecords;
  }

  public long getReduceOutputRecords() {
    return reduceOutputRecords;
  }

  public long getProactiveSpillCountObjects() {
    return proactiveSpillCountObjects;
  }

  public long getProactiveSpillCountRecs() {
    return proactiveSpillCountRecs;
  }

  public long getRecordsWritten() {
    return recordsWritten;
  }

  public long getSmmSpillCount() {
    return smmSpillCount;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public List<PigIoStats> getInputStats() {
    return inputStats;
  }

  public List<PigIoStats> getOutputStats() {
    return outputStats;
  }

  private static List<Object> statsToJson(List<PigIoStats> stats) {
    List<Object> jsonObj = new ArrayList<Object>();
    for (PigIoStats stat : stats) {
      jsonObj.add(stat.toJson());
    }
    return jsonObj;
  }

  private static List<PigIoStats> statsFromJson(Object obj) {
    List<PigIoStats> stats = new ArrayList<PigIoStats>();

    @SuppressWarnings("unchecked")
    List<Object> jsonStats = (ArrayList<Object>) obj;

    for (Object jsonStat : jsonStats) {
      stats.add(PigIoStats.fromJson(jsonStat));
    }
    return stats;
  }

  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("numberMaps", Integer.toString(numberMaps));
    jsonObj.put("numberReduces", Integer.toString(numberReduces));
    jsonObj.put("minMapTime", Long.toString(minMapTime));
    jsonObj.put("maxMapTime", Long.toString(maxMapTime));
    jsonObj.put("avgMapTime", Long.toString(avgMapTime));
    jsonObj.put("minReduceTime", Long.toString(minReduceTime));
    jsonObj.put("maxReduceTime", Long.toString(maxReduceTime));
    jsonObj.put("avgReduceTime", Long.toString(avgReduceTime));
    jsonObj.put("bytesWritten", Long.toString(bytesWritten));
    jsonObj.put("hdfsBytesWritten", Long.toString(hdfsBytesWritten));
    jsonObj.put("mapInputRecords", Long.toString(mapInputRecords));
    jsonObj.put("mapOutputRecords", Long.toString(mapOutputRecords));
    jsonObj.put("reduceInputRecords", Long.toString(reduceInputRecords));
    jsonObj.put("reduceOutputRecords", Long.toString(reduceOutputRecords));
    jsonObj.put("proactiveSpillCountObjects",
        Long.toString(proactiveSpillCountObjects));
    jsonObj.put("proactiveSpillCountRecs",
        Long.toString(proactiveSpillCountRecs));
    jsonObj.put("recordsWritten", Long.toString(recordsWritten));
    jsonObj.put("smmSpillCount", Long.toString(smmSpillCount));
    jsonObj.put("errorMessage", errorMessage);
    jsonObj.put("inputStats", statsToJson(inputStats));
    jsonObj.put("outputStats", statsToJson(outputStats));
    return jsonObj;
  }

  @SuppressWarnings("unchecked")
  public static PigJobStats fromJson(Object obj) throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    int numberMaps = Integer.parseInt((String) jsonObj.get("numberMaps"));
    int numberReduces = Integer.parseInt((String) jsonObj.get("numberReduces"));
    long minMapTime = Long.parseLong((String) jsonObj.get("minMapTime"));
    long maxMapTime = Long.parseLong((String) jsonObj.get("maxMapTime"));
    long avgMapTime = Long.parseLong((String) jsonObj.get("avgMapTime"));
    long minReduceTime = Long.parseLong((String) jsonObj.get("minReduceTime"));
    long maxReduceTime = Long.parseLong((String) jsonObj.get("maxReduceTime"));
    long avgReduceTime = Long.parseLong((String) jsonObj.get("avgReduceTime"));
    long bytesWritten = Long.parseLong((String) jsonObj.get("bytesWritten"));
    long hdfsBytesWritten =
        Long.parseLong((String) jsonObj.get("hdfsBytesWritten"));
    long mapInputRecords =
        Long.parseLong((String) jsonObj.get("mapInputRecords"));
    long mapOutputRecords =
        Long.parseLong((String) jsonObj.get("mapOutputRecords"));
    long reduceInputRecords =
        Long.parseLong((String) jsonObj.get("reduceInputRecords"));
    long reduceOutputRecords =
        Long.parseLong((String) jsonObj.get("reduceOutputRecords"));
    long proactiveSpillCountObjects =
        Long.parseLong((String) jsonObj.get("proactiveSpillCountObjects"));
    long proactiveSpillCountRecs =
        Long.parseLong((String) jsonObj.get("proactiveSpillCountRecs"));
    long recordsWritten =
        Long.parseLong((String) jsonObj.get("recordsWritten"));
    long smmSpillCount = Long.parseLong((String) jsonObj.get("smmSpillCount"));
    List<PigIoStats> inputs = statsFromJson(jsonObj.get("inputStats"));
    List<PigIoStats> outputs = statsFromJson(jsonObj.get("outputStats"));
    String errorMessage = (String) jsonObj.get("errorMessage");

    return new PigJobStats(numberMaps, numberReduces, minMapTime, maxMapTime,
        avgMapTime, minReduceTime, maxReduceTime, avgReduceTime, bytesWritten,
        hdfsBytesWritten, mapInputRecords, mapOutputRecords,
        reduceInputRecords, reduceOutputRecords, proactiveSpillCountObjects,
        proactiveSpillCountRecs, recordsWritten, smmSpillCount, errorMessage,
        inputs, outputs);
  }
}
