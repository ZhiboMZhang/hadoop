/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.workflow.examples.jobs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Dummy class for testing MR framefork. Sleeps for a defined period of time in
 * mapper and reducer. In addition to sleeping, it passes through input data
 * while appending an indication that the data was received.
 */
public class SleepJob extends Configured implements Tool,
    Mapper<LongWritable, Text, Text, Text>, Reducer<Text, Text, Text, Text> {

  private static long TWENTY_SECONDS = 20000L;

  private long mapSleepDuration = TWENTY_SECONDS;
  private long reduceSleepDuration = TWENTY_SECONDS;
  private String jobName = "N/A";

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    try {
      Thread.sleep(mapSleepDuration);
    } catch (InterruptedException ie) {
      throw new IOException("Map task for " + jobName
          + " interrupted while sleeping.");
    }

    // The keys are the position in the file, which we don't care about.
    // Values are a line of text.
    // Test input is a single line, add that this task/job processed the line.
    Text status = new Text(jobName + "-map");
    output.collect(status, value);
  }

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    try {
      Thread.sleep(reduceSleepDuration);
    } catch (InterruptedException ie) {
      throw new IOException("Reduce task for " + jobName
          + " interrupted while sleeping.");
    }

    // The keys are the job information. Continue the chain by prepending the
    // previous tasks to the value and putting ourself as the key.
    Text status = new Text(jobName + "-red");
    while (values.hasNext()) {
      // A single task executes once, so this shouldn't loop.
      Text newValue = new Text(key.toString() + " " + values.next().toString());
      output.collect(status, newValue);
    }
  }


  @Override
  public void configure(JobConf job) {
    this.mapSleepDuration = job.getLong("sleep.job.map.sleep.time", TWENTY_SECONDS);
    this.reduceSleepDuration = job.getLong("sleep.job.reduce.sleep.time", TWENTY_SECONDS);
    this.jobName = job.getJobName();
  }

  @Override
  public void close() throws IOException {}

  public static void main(String[] args) throws Exception{
    ToolRunner.run(new Configuration(), new SleepJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("SleepJob <input> <output> <numMapper> <numReducer>"
          + " <mapSleepTime (msec)> <reduceSleepTime (msec)>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    int numMapper = Integer.parseInt(args[2]);
    int numReducer = Integer.parseInt(args[3]);
    long mapSleepTime = TWENTY_SECONDS;
    long reduceSleepTime = TWENTY_SECONDS;

    if (args.length > 4) {
      mapSleepTime = Long.parseLong(args[4]);
      reduceSleepTime = Long.parseLong(args[5]);
    }

    JobConf conf = new JobConf(getConf(), SleepJob.class);

    conf.setLong("sleep.job.map.sleep.time", mapSleepTime);
    conf.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);

    conf.setNumMapTasks(numMapper);
    conf.setNumReduceTasks(numReducer);
    
    conf.setMapperClass(SleepJob.class);
    conf.setReducerClass(SleepJob.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setSpeculativeExecution(false);

    // When running as a workflow job these are reset before job submission to
    // the value given in the jar manifest.
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

    return 0;
  }

}