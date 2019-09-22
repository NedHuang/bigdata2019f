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


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);


  /**************************************************************************
  * Word
  ***************************************************************************/
  public static final class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  // Mapper: emits (token, 1) for every word occurrence.  from WordCount.java 
  
    // private static final FloatWritable ONE = new FloatWritable(1); //A WritableComparable for floats, do we need float here?
    // private static final PairOfStrings BIGRAM = new PairOfStrings();
    // Reuse objects to save overhead of object creation.
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
      line_conter.increment(1);

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
  //输出<'a',1>。。。




/**************************************************************************
 * Pair
***************************************************************************/
  public static final class PairMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    // emit (Pair->key, One->value) , e.g., (('aaa','bbb'),1)
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      int tokenCounter = 0;
      Set<String> pairSet = new HashSet<String>();
      
      // add the first 40 word into a set，数前40个
      for(String word : Tokenizer.tokenize(value.toString())){
        tokenCounter ++;
        if(tokenCounter > 40) break;
        pairSet.add(word);
      }

      // add words into pair
      for(String leftWord : pairSet){
        for(String rightWord : pairSet){
          PAIR.set(leftWord,rightWord);
          context.write(PAIR,ONE);
          
          // context.write(PAIR,ONE); 
        }
      }
    }
  }

  //https://github.com/lintool/bespin/blob/master/src/main/java/io/bespin/java/mapreduce/bigram/ComputeBigramRelativeFrequencyPairs.java
  private static final class PairCombiner extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
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


  // combiner In the final output, the key should be a word (e.g., A), and the value should be a map, 
  // where each of the keys is a co-occurring word (e.g., B), and the value is a pair (PMI, count) denoting its PMI and co-occurrence count.
  // modifiey from WordCombiner  Myreducer in WordCount.java
  public static final class PairReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {

    private static final PairOfFloatInt PMI = new PairOfFloatInt();
    private static final Map<String, Integer> wordCount = new HashMap<String, Integer>();

    private static long numLines;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numLines = conf.getLong("counter", 0L);
      //read from file
      FileSystem fileSystem = FileSystem.get(conf);
      FileStatus[] status = fileSystem.globStatus(new Path("tmp/part-r-*"));
      for (FileStatus file : status) {
        FSDataInputStream dataInput = fileSystem.open(file.getPath());
        InputStreamReader inputStreamReader = new InputStreamReader(dataInput, "UTF-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line = bufferedReader.readLine();
        while (line != null) {
          String[] data = line.split("\\s+");
          if (data.length == 2) {
            wordCount.put(data[0], Integer.parseInt(data[1]));
          }
          line = bufferedReader.readLine();
        } 
        bufferedReader.close();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
      numLines = conf.getLong("counter", 0L);
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }


      int threshold = conf.getInt("threshold", 0);

      if (sum >= threshold) {
        String left = key.getLeftElement();
        String right = key.getRightElement();

        float pmi = (float) Math.log10((double)(sum * numLines) / (double)(wordCount.get(left) * wordCount.get(right)));
        PMI.set(pmi, sum);
        context.write(key, PMI);
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
  private PairsPMI() {
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

    String tempPath = "tmp/";

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
    // write into tempPath
    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(tempPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WordMapper.class);
    job.setCombinerClass(WordReducer.class);
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


    // Second Job
    long count = job.getCounters().findCounter(WordMapper.LineCounter.lineCounter).getValue();
    conf.setLong("counter", count);
    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job2.setMapperClass(PairMapper.class);
    job2.setCombinerClass(PairCombiner.class);
    job2.setReducerClass(PairReducer.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
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
    ToolRunner.run(new PairsPMI(), args);
  }
}