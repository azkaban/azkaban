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
package azkaban.jobtype.examples.java;

import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.utils.Props;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCount extends AbstractHadoopJob {

  private final static Logger LOG = LoggerFactory.getLogger(WordCount.class);

  private final String inputPath;
  private final String outputPath;
  private boolean forceOutputOverwrite;

  public WordCount(String name, Props props) {
    super(name, props);
    this.inputPath = props.getString("input.path");
    this.outputPath = props.getString("output.path");
    this.forceOutputOverwrite =
        props.getBoolean("force.output.overwrite", false);
  }

  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {

    enum Counters {
      INPUT_WORDS
    }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private long numRecords = 0;

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        output.collect(word, one);
        reporter.incrCounter(Counters.INPUT_WORDS, 1);
      }

      if ((++numRecords % 100) == 0) {
        reporter.setStatus("Finished processing " + numRecords + " records "
            + "from the input file");
      }
    }
  }

  public static class Reduce extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  @Override
  public void run() throws Exception {
    LOG.info(String.format("Starting %s", getClass().getSimpleName()));

    // hadoop conf should be on the classpath
    JobConf jobconf = getJobConf();
    jobconf.setJarByClass(WordCount.class);

    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(IntWritable.class);

    jobconf.setMapperClass(Map.class);
    jobconf.setReducerClass(Reduce.class);

    jobconf.setInputFormat(TextInputFormat.class);
    jobconf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.addInputPath(jobconf, new Path(inputPath));
    FileOutputFormat.setOutputPath(jobconf, new Path(outputPath));

    if (forceOutputOverwrite) {
      FileSystem fs =
          FileOutputFormat.getOutputPath(jobconf).getFileSystem(jobconf);
      fs.delete(FileOutputFormat.getOutputPath(jobconf), true);
    }

    super.run();
  }
}
