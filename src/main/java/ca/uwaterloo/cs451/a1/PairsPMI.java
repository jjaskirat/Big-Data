package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  public static final class MyMapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    public enum MyCounter { LINE_COUNTER };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int num = 0;
      Set<String> uniqueWords = new HashSet<String>();
      for (String token : Tokenizer.tokenize(value.toString())) {
        uniqueWords.add(token);
        num++;
        if (num >= 40) break;
      }

      String[] words = new String[uniqueWords.size()];
      words = uniqueWords.toArray(words);

      for (int i = 0; i < words.length; i++) {
        WORD.set(words[i]);
        context.write(WORD, ONE);
      }

      Counter counter = context.getCounter(MyCounter.LINE_COUNTER);
      counter.increment(1L);
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyReducerWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  public static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final PairOfStrings PAIR = new PairOfStrings();
     private int window = 2;

    
    
    @Override
    public void setup(Context context) {
      window = context.getConfiguration().getInt("window", 2);
}

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int num = 0;
      Set<String> uniqueWords = new HashSet<String>();
      for (String token : Tokenizer.tokenize(value.toString())) {
        uniqueWords.add(token);
        num++;
        if (num >= 40) break;
      }

      String[] tokens = new String[uniqueWords.size()];
      tokens = uniqueWords.toArray(tokens);

      for (int i = 0; i < tokens.length; i++) {
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.length); j++) {
          if (i == j) continue;
          PAIR.set(tokens[i], tokens[j]);
          context.write(PAIR, ONE);
        }
      }
}
  }

  public static final class MyCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  public static final class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {

    private static final PairOfFloatInt PMI = new PairOfFloatInt();
    private static final Map<String, Integer> allWords = new HashMap<String, Integer>();

    private static long partLines = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      partLines = conf.getLong("counter", 0L);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] status = fs.globStatus(new Path("temp/part-r-*"));
      for (FileStatus file : status) {
        FSDataInputStream is = fs.open(file.getPath());
        InputStreamReader isr = new InputStreamReader(is, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();
        while (line != null) {
          String[] data = line.split("\\s+");
          if (data.length == 2) {
            allWords.put(data[0], Integer.parseInt(data[1]));
          }
          line = br.readLine();
        }
        br.close();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);

      if (sum >= threshold) {
        String left = key.getLeftElement();
        String right = key.getRightElement();

        float pmi = (float) Math.log10((double)(sum * partLines) / (double)(allWords.get(left) * allWords.get(right)));
        PMI.set(pmi, sum);
        context.write(key, PMI);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    String tempPath = "temp/";

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.set("tempPath", tempPath);
    conf.set("threshold", Integer.toString(args.threshold));
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(tempPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapperWordCount.class);
    job.setCombinerClass(MyReducerWordCount.class);
    job.setReducerClass(MyReducerWordCount.class);

//     job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
//     job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
//     job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
//     job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
//     job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(tempPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
        long totalTime = System.currentTimeMillis();

    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Second Job
    long count = job.getCounters().findCounter(MyMapperWordCount.MyCounter.LINE_COUNTER).getValue();
    conf.setLong("counter", count);
    Job Job2 = Job.getInstance(conf);
    Job2.setJobName(PairsPMI.class.getSimpleName());
    Job2.setJarByClass(PairsPMI.class);

    Job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(Job2, new Path(args.input));
    FileOutputFormat.setOutputPath(Job2, new Path(args.output));

    Job2.setMapOutputKeyClass(PairOfStrings.class);
    Job2.setMapOutputValueClass(IntWritable.class);
    Job2.setOutputKeyClass(PairOfStrings.class);
    Job2.setOutputValueClass(PairOfFloatInt.class);
    Job2.setOutputFormatClass(TextOutputFormat.class);

//     secondJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
//     secondJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
//     secondJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
//     secondJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
//     secondJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    Job2.setMapperClass(MyMapper.class);
    Job2.setCombinerClass(MyCombiner.class);
    Job2.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    Job2.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    LOG.info("Total Time " + (System.currentTimeMillis() - totalTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
