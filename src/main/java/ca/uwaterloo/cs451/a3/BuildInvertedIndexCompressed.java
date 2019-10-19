/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a3;


import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import java.io.*;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final PairOfStringInt WORD = new PairOfStringInt();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
//         String temp = e.getLeftElement() + " " + (int) docno.get();
//         WORD.set(temp);
        WORD.set(e.getLeftElement(), (int) docno.get());
        context.write(WORD, new IntWritable(e.getRightElement()));
      }
    }
  }


  protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    private final static IntWritable DF = new IntWritable();
    private final static Text WORD = new Text("");
    private final static ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private final static DataOutputStream postings = new DataOutputStream(bos);

    String prev = "";
    //     private static final ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

    int prevDocNo = 0;
    int curDocNo = 0;
        int df = 0;


    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();

      if (!key.getLeftElement().equals(prev) && !prev.equals("")) {
        //         DF.set(df);
//         context.write(new Text(prev), new PairOfWritables<>(DF, postings));
        postings.flush();
        bos.flush();
        ByteArrayOutputStream bos2 = new ByteArrayOutputStream(bos.size());
        DataOutputStream MyPair = new DataOutputStream(bos2);
        WritableUtils.writeVInt(MyPair, df);
        //         context.write(new Text(prev), new BytesWritable(bos.toByteArray()));

        MyPair.write(bos.toByteArray());

        WORD.set(prev);
        context.write(WORD, new BytesWritable(bos2.toByteArray()));

        bos.reset();
        //         postings.clear();

        prevDocNo = 0;
        df = 0;
      }

      //only loops once
      while (iter.hasNext()) {
        df++;
        curDocNo = key.getRightElement();
//         WritableUtils.writeVInt(postings, (curDocNo));
        WritableUtils.writeVInt(postings, (curDocNo - prevDocNo));
        WritableUtils.writeVInt(postings, iter.next().get());
        prevDocNo = curDocNo;
      }
      prev = key.getLeftElement();
       // Sort the postings by docno ascending.
//       Collections.sort(postings);

//       DF.set(df);
//       context.write(new Text(newKey), new PairOfWritables<>(DF, postings));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      //       DF.set(df);
//       context.write(new Text(prev), new PairOfWritables<>(DF, postings));
      postings.flush();
      bos.flush();

      ByteArrayOutputStream bos2 = new ByteArrayOutputStream(bos.size());
      DataOutputStream MyPair = new DataOutputStream(bos2);
      WritableUtils.writeVInt(MyPair, df);
      MyPair.write(bos.toByteArray());

      WORD.set(prev);
      context.write(WORD, new BytesWritable(bos2.toByteArray()));

      bos.close();
      postings.close(); 
    }
  }

  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class); //delete

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
