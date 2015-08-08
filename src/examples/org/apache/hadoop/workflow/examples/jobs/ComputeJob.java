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
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * Dummy class for testing the MapReduce framework.
 *
 * Computes an approximation of Pi with respect to a certain margin of error in
 * the mapper. In addition to this computation, it passes through input data
 * while appending an indication that the data was received.
 */
public class ComputeJob extends Configured implements Tool,
    Mapper<LongWritable, Text, Text, Text>, Reducer<Text, Text, Text, Text> {

  private static final Log LOG = LogFactory.getLog(ComputeJob.class);

  private float marginOfError = 0.001f;
  private String jobName = "N/A";

  private void approximatePi(double marginOfError) {

    double sum = 0d;
    int counter = 0;

    do {
      sum += (Math.pow(-1, counter) / ((2 * counter) + 1));
      counter++;

    } while (((sum * 4) < (Math.PI - marginOfError))
        || ((sum * 4) > (Math.PI + marginOfError)));
  }

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    // Do some computation.
    approximatePi(marginOfError);

    // The keys are the position in the file, which we don't care about.
    // Values are a line of text.
    // Test input is a single line, add that this task/job processed the line.
    Text status = new Text(jobName + "-map");
    output.collect(status, value);
  }

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    // Do some computation.
    approximatePi(marginOfError);

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
    this.marginOfError = job.getFloat("compute.job.error", this.marginOfError);
    this.jobName = job.getJobName();
  }

  @Override
  public void close() throws IOException {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new ComputeJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("ComputeJob <input> <output>"
          + " <numMapper> <numReducer> <marginOfError>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = Integer.parseInt(args[2]);
    int numReducer = Integer.parseInt(args[3]);
    if (args.length > 4) { marginOfError = Float.parseFloat(args[4]); }

    JobConf conf = new JobConf(getConf(), ComputeJob.class);

    conf.setFloat("compute.job.error", marginOfError);

    conf.setNumMapTasks(numMapper);
    conf.setNumReduceTasks(numReducer);

    conf.setMapperClass(ComputeJob.class);
    conf.setReducerClass(ComputeJob.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setSpeculativeExecution(false);

    // When running as a workflow job these are reset before job submission to
    // the value given in the jar manifest.
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Record duration for testing.
    Date startTime = new Date();
    LOG.info("TESTING: Job " + conf.getJobName() + " started: " + startTime);

    JobClient.runJob(conf);

    Date endTime = new Date();
    LOG.info("TESTING: Job " + conf.getJobName() + " ended: " + endTime);

    long duration = endTime.getTime() - startTime.getTime();
    LOG.info("TESTING: Job " + conf.getJobName() + " took " + (duration / 1000)
        + " seconds (" + duration + " ms).");

    return 0;
  }

}