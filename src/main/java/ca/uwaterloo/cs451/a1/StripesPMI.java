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
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class StripesPMI extends Configured implements Tool {
 private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class MyMapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
	      public enum MyCounter { LINE_COUNTER };


    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
	Set<String> hash_Set = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
	      hash_Set.add(word);
      }
	    for(String word : hash_Set){
        WORD.set(word);
        context.write(WORD, ONE);
      }
    }
}
	
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


  public static final class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();
	  
	      public enum MyCounter { LINE_COUNTER };


    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWords = 0;
      Set<String> set = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
        set.add(word);
        numWords++;
        if (numWords >= 40) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);

      for (int i = 0; i < words.length; i++) {
        MAP.clear();
        for (int j = 0; j < words.length; j++) {
          if (i == j) continue;
          MAP.increment(words[j]);
        }

        KEY.set(words[i]);
        context.write(KEY, MAP);
      }
    }
  }

  public static final class MyCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  public static final class MyReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {
    private static final Text KEY = new Text();
    private static final HashMapWritable finalMap = new HashMapWritable();
    private static int totalSum = 0;
    private static int threshold = 0;
    
    private static final Map<String, Integer> total = new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException{
      //TODO Read from intermediate output of first job
      // and build in-memory map of terms to their individual totals
	    	    threshold = context.getConfiguration().getInt("threshold", 3);

      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      
      String yoPath = conf.get("intermediatePath");
      Path filePath = new Path(yoPath + "/part-r-*");
	    

      if(!fs.exists(filePath)){
        throw new IOException("File Not Found: ");
      }
      
      BufferedReader reader = null;
      try{
        FSDataInputStream fin = fs.open(filePath);
        InputStreamReader inStream = new InputStreamReader(fin);
        reader = new BufferedReader(inStream);
        
      } catch(Exception e){
        throw new IOException("Can not open file");
      }
      
      
      String line = reader.readLine();
      while(line != null){
        
        String[] parts = line.split("\\s+");
        if(parts.length != 2){
          LOG.info("incorrect format");
        } else {
          total.put(parts[0], Integer.parseInt(parts[1]));
		totalSum += Integer.parseInt(parts[1]);
        }
        line = reader.readLine();
      }
      
      reader.close();
      
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);

      String left = key.toString();
      KEY.set(left);
      finalMap.clear();
      for (String currentKey : map.keySet()) {
          int sum = map.get(currentKey);
          float pmi = (float) Math.log10((double)(sum * totalSum) / (double)(total.get(left) * total.get(currentKey)));
          PairOfFloatInt PMI_COUNT = new PairOfFloatInt();
	      if(sum > threshold){
          PMI_COUNT.set(pmi, sum);
          finalMap.put(currentKey, PMI_COUNT);
        }
      }
      if (finalMap.size() > 0) {
        context.write(KEY, finalMap);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

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

    String tempPath = "temp";

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
        Path tempDir = new Path(tempPath);

    conf.set("intermediatePath", tempPath);
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName() + "Word Count");
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(1);

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
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Second Job
//     long count = job.getCounters().findCounter(MyMapper.MyCounter.LINE_COUNTER).getValue();
//     conf.setLong("counter", count);
    Job secondJob = Job.getInstance(conf);
    secondJob.setJobName(StripesPMI.class.getSimpleName() + "StripesPMI");
    secondJob.setJarByClass(StripesPMI.class);

    secondJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(secondJob, new Path(args.input));
    FileOutputFormat.setOutputPath(secondJob, new Path(args.output));

    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(Text.class);
    secondJob.setOutputValueClass(HashMapWritable.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    secondJob.setMapperClass(MyMapper.class);
    secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(MyReducer.class);

//     secondJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
//     secondJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
//     secondJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
//     secondJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
//     secondJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
