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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;

// A frequency distribution where events are arbitrary objects and counts are ints.
import tl.lin.data.fd.Object2IntFrequencyDistribution;
// Implementation of {@link Object2LongFrequencyDistribution} based on {@link HMapKL}.

import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;


import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// my import
import org.apache.hadoop.io.WritableUtils;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import java.io.File; 
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream; 





public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  // mapper
  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    // private static final Text WORD = new Text();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    // docno should be the gap for compression.
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token); // word and tf of the word
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        // ((WORD, docno), tf), e.g., (('fish',1), 5)
        context.write(new PairOfStringInt(e.getLeftElement(), (int) docno.get()), new IntWritable(e.getRightElement()));
      }
    }
  }


  // partitioner
  protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }



  private static final class MyReducer extends Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {

    // private static final IntWritable DF = new IntWritable(); // int is enough
    private int df = 0; 
    private static final Text WORD = new Text();

    // this is used to write binary 
    private static ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    private static DataOutputStream dataOut = new DataOutputStream(byteOutput);

    // postings( binary )
    private static ByteArrayOutputStream bytePosting = new ByteArrayOutputStream();
    private static DataOutputStream posting = new DataOutputStream(bytePosting);

    // prevKey: the previous keyword and previous document number, if prevKeyWord changed, write and reset.
    private String prevKeyWord = null;
    private int prevDocNo = 0;


    @Override
    // key = (word, tf) values: docno
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      Iterator<IntWritable> iter = values.iterator();
      
      // if the wordsare different
      if(!key.getLeftElement().equals(prevKeyWord) && prevKeyWord != null){
        //forces data to the underlying file output stream
        bytePosting.flush();
        posting.flush();

        // (DataOutputStream stream , int i. write df into dataOut)
        WritableUtils.writeVInt(dataOut, df);
        dataOut.write(bytePosting.toByteArray());
        byteOutput.flush();
        dataOut.flush();

        WORD.set(prevKeyWord);

        // convert to bytearray and write to context;
        BytesWritable byteWritablePosting = new BytesWritable(byteOutput.toByteArray());
        context.write(WORD, byteWritablePosting);

        // reset ByteArrayOutputStream
        bytePosting.reset();
        byteOutput.reset();
        // reset preDocNo and df
        prevDocNo = 0;
        df = 0;
      }

      prevKeyWord = key.getLeftElement(); // reset the prevKeyword

      // if there is NEXT...
      while (iter.hasNext()) {
        // postings.add(iter.next().clone());// too expensive, write as byte
        // df++;

        // get thisDocNo and calculate gap, get the tf
        int thisDocNo = key.getRightElement();
        int gap = thisDocNo - prevDocNo;
        int tf = iter.next().get();
        // (DataOutputStream stream , int i. write df into dataOut)
        WritableUtils.writeVInt(posting, gap);    //  write gap 
        WritableUtils.writeVInt(posting, tf);     //  write tf 
        df ++;
      }
    }  

    // deal with the last docNo and tf in iter....
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
      super.cleanup(context);

      bytePosting.flush();
      posting.flush();

      // (DataOutputStream stream , int i. write df into dataOut)
      WritableUtils.writeVInt(dataOut, df);
      dataOut.write(bytePosting.toByteArray());
      byteOutput.flush();
      dataOut.flush();

      WORD.set(prevKeyWord);

      // write byteOutput to context
      BytesWritable byteWritablePosting = new BytesWritable(byteOutput.toByteArray());
      context.write(WORD, byteWritablePosting);

      // close ByteArrayOutputStream
      bytePosting.close();
      posting.close();


    
    }

  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
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

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

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
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}