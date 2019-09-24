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

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper.Counter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfStrings;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;


public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);


  /**************************************************************************
  * WordStripesPMI
  ***************************************************************************/
  // from PairsPMI
  public static final class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    // user-defined counter, count lines. https://hadoop.apache.org/docs/r2.7.4/api/org/apache/hadoop/mapred/Counters.html
    public enum LineCounter{
      lineCounter;
    }
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int tokenCounter = 0;
      Set<String> tokenSet = new HashSet<String>();
      
      // count the line, increment the counter by 1
      Counter line_conter = context.getCounter(LineCounter.lineCounter);
      line_conter.increment(1L);

      for(String word : Tokenizer.tokenize(value.toString())) {
        tokenCounter++;
        // consider up to only the first 40 words on each line~~
        if(tokenCounter > 40) break;
        // add the token to set, and emit(token,1) for each unique token occurance
        if(tokenSet.add(word)){
          WORD.set(word);
          context.write(WORD, ONE);
        }
      }
    }
  }

  //WordCombiner
  private static final class WordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // WordReducer: sums up all the counts.
  public static final class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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




/**************************************************************************
 * Stripes
***************************************************************************/
  public static final class StripeMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    // emit (mainKey,(k1,v1)) etc ('cs651', ("hao'ma'fan'a",666))
    private static final HMapStIW MAP = new HMapStIW();  //<String,Integer>
    private static final Text mainKey = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      int tokenCounter = 0;
      Set<String> keyStripe = new HashSet<String>();
      
      // add the first 40 word into a set，数前40个
      for(String word : Tokenizer.tokenize(value.toString())){
        tokenCounter ++;
        if(tokenCounter > 40) break;
        keyStripe.add(word);
      }
      // keyStripe = {'a','b','c',...}

      // add words into stripe
      for(String leftWord : keyStripe){
        MAP.clear(); // clear the value (k1,v1)
        for(String rightWord : keyStripe){
          MAP.increment(rightWord);
        }
        mainKey.set(leftWord);
        context.write(mainKey, MAP);
      }
    }
  }

  //Combiner
  public static final class StripeCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
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


  // combiner In the final output, the key should be a word (e.g., A), and the value should be a map, 
  // where each of the keys is a co-occurring word (e.g., B), and the value is a pair (PMI, count) denoting its PMI and co-occurrence count.
  // modifiey from WordCombiner  Myreducer in WordCount.java
  public static final class StripeReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {
    private static final Text KEY = new Text();
    private static final HashMapWritable MAP = new HashMapWritable();
    
    private static final Map<String, Integer> wordCount = new HashMap<String, Integer>();
    private static long number_of_lines;

    @Override
    // call once at the start
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      //initialize number_of_lines to 0
      number_of_lines = conf.getLong("counter", 0L);
      //read from file, get how many times each word appears
      
      
      FileSystem fileSystem = FileSystem.get(conf);
      Path temp_file_path = new Path("tempPath/part-r-*");
      FileStatus[] status = fileSystem.globStatus(temp_file_path);
      

      for (FileStatus file : status) {
        FSDataInputStream dataInput = fileSystem.open(file.getPath());
        InputStreamReader inputStreamReader = new InputStreamReader(dataInput, "UTF-8"); // "UTF-8"
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line = bufferedReader.readLine();
        while (line != null) {
          String[] word_and_count = line.split("\\s+"); // \\s doesn't work, there might be more whitespaces
          // has to filter something awkward
          if (word_and_count.length == 2) {
            wordCount.put(word_and_count[0], Integer.parseInt(word_and_count[1]));
          }
          line = bufferedReader.readLine();
        } 
        bufferedReader.close();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      // from combiner
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);
      number_of_lines = conf.getLong("counter", 0L);
      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      String left = key.toString();
      KEY.set(left);
      MAP.clear();
      for (String right : map.keySet()) {
        if (map.get(right) >= threshold) {
          int pair_co_occurance = map.get(right);
          // gives NaN
          // float numerator = (float)(pair_co_occurance / number_of_lines);
          // float denominator = (double)(wordCount.get(left) / number_of_lines
          // wordCount.get(right) / number_of_lines);
          // float pmi = (float) Math.log10(numerator / denominator);
          float StripePmi = (float) Math.log10((double)(pair_co_occurance)*number_of_lines / (double)(wordCount.get(left) * wordCount.get(right)));
          PairOfFloatInt PMI_COUNT = new PairOfFloatInt();
          PMI_COUNT.set(StripePmi, pair_co_occurance);
          MAP.put(right, PMI_COUNT);
        }
      }
      if (MAP.size() > 0) {
        context.write(KEY, MAP);
      }
    }
  }


  //https://github.com/lintool/bespin/blob/master/src/main/java/io/bespin/java/mapreduce/bigram/ComputeBigramRelativeFrequencyPairs.java
  private static final class PairPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }




  
  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {
  }

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    boolean textOutput = false;

    @Option(name = "-threshold", usage = "threshold of co-occurrence")
    int threshold = 0;
  }



  // https://netjs.blogspot.com/2018/07/chaining-mapreduce-job-in-hadoop.html
//   @Override
//   public int run(String[] argv) throws Exception{
//     final Args args = new Args();
//     CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

//     try {
//       parser.parseArgument(argv);
//     } catch (CmdLineException e) {
//       System.err.println(e.getMessage());
//       parser.printUsage(System.err);
//       return -1;
//     }

//     LOG.info("Tool: " + PairsPMI.class.getSimpleName());
//     LOG.info(" - input path: " + args.input);
//     LOG.info(" - output path: " + args.output);
//     LOG.info(" - number of reducers: " + args.numReducers);
//     LOG.info(" - threshold: " + args.threshold);

//     Configuration conf = getConf();
//     Job job = Job.getInstance(conf);
//     job.setJarByClass(getClass());
//     // MapReduce chaining
//     Configuration mapConf1 = new Configuration(false);
//     ChainMapper.addMapper(job, WordMapper.class, LongWritable.class, Text.class,
//                 Text.class, Text.class,  mapConf1);
     
//     Configuration mapConf2 = new Configuration(false);

//     ChainMapper.addMapper(job, PairMapper.class, Text.class, Text.class,
//               Text.class, IntWritable.class, mapConf2);
    
//     Configuration reduceConf = new Configuration(false);        
//     ChainReducer.setReducer(job, WordReducer.class, Text.class, IntWritable.class,
//             Text.class, IntWritable.class, reduceConf);

//     ChainReducer.addMapper(job, InverseMapper.class, Text.class, IntWritable.class,
//             IntWritable.class, Text.class, null);
    
//     job.setOutputKeyClass(IntWritable.class);
//     job.setOutputValueClass(Text.class);
//     FileInputFormat.addInputPath(job, new Path(args[0]));
//     FileOutputFormat.setOutputPath(job, new Path(args[1]));
//     return job.waitForCompletion(true) ? 0 : 1;
//  }
    
//     return 0;
//   }


  // https://stackoverflow.com/questions/29741305/how-can-i-have-multiple-mappers-and-reducers connects two jobs
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    String tempPath = "tempPath/";

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
    JobConf jconf = new JobConf(PairsPMI.class); // seems to be useless
    conf.set("threshold", Integer.toString(args.threshold));
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);
    // write into tempPath
    FileInputFormat.setInputPaths(job, new Path(args.input));
    // FileOutputFormat.setOutputPath(job, new Path(args.output));
    FileOutputFormat.setOutputPath(job, new Path(tempPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WordMapper.class);
    // job.setCombinerClass(WordReducer.class);
    job.setReducerClass(WordReducer.class);

    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(tempPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Pair
    // Second Job
    Configuration conf_pair = getConf();
    JobConf jconf_pair = new JobConf(PairsPMI.class); // seems to be useless


    long count = job.getCounters().findCounter(WordMapper.LineCounter.lineCounter).getValue();
    // set a counter to count number of lines
    conf.setLong("counter", count);
    Job job_stripe = Job.getInstance(conf);
    job_stripe.setJobName(PairsPMI.class.getSimpleName());
    job_stripe.setJarByClass(PairsPMI.class);

    job_stripe.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_stripe, new Path(args.input));
    FileOutputFormat.setOutputPath(job_stripe, new Path(args.output));

    //cange
    job_stripe.setMapOutputKeyClass(Text.class);
    job_stripe.setMapOutputValueClass(HMapStIW.class);
    job_stripe.setOutputKeyClass(Text.class);
    job_stripe.setOutputValueClass(HashMapWritable.class);
    job_stripe.setOutputFormatClass(TextOutputFormat.class);

    job_stripe.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job_stripe.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job_stripe.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job_stripe.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job_stripe.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job_stripe.setMapperClass(StripeMapper.class);
    // job_stripe.setCombinerClass(StripeCombiner.class);
    job_stripe.setReducerClass(StripeReducer.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    job_stripe.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
