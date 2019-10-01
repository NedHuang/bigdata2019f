 
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

  package ca.uwaterloo.cs451.a2;

  import io.bespin.scala.util.Tokenizer

  import org.apache.log4j._
  import org.apache.hadoop.fs._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.rogach.scallop._
  import org.apache.spark.Partitioner
  import org.apache.spark.HashPartitioner
  
  // no need to change
  class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, reducers)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    val threshold = opt[Int](descr = "threshold", required = false, default = Some(0) )
    val executorCores = opt[Int](descr = "executor-cores", required = true, default = Some(2))
    val numExecutors = opt[Int](descr = "num-executors", required = true, default = Some(4))
    val executorMemory = opt[String](descr = "executor-memory", required = true, default = Some("24G"))

    verify()
  }
  
  class PairsPMIPartitioner(partitions: Int) extends Partitioner {
    def numPartitions: Int = partitions
    def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[(String, String)]
      ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
    }
  }

  object PairsPMI extends Tokenizer {
    val log = Logger.getLogger(getClass().getName())
  
    def main(argv: Array[String]) {
      print("Hello World")
      val args = new PairsPMIConf(argv)
  
      log.info("Input: " + args.input())
      log.info("Output: " + args.output())
      log.info("Number of reducers: " + args.reducers())
  
      val conf = new SparkConf().setAppName("PairsPMI")
      val sc = new SparkContext(conf)
  
      val outputDir = new Path(args.output())
      FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

      var sum = 0.0f
      val textFile = sc.textFile(args.input())
      var numOfLines = textFile.count()

      val threshold = args.threshold()
      
      val words = textFile.flatMap(
        line => {
          val tokens = tokenize(line)
          if (tokens.length > 0){
            tokens.take(Math.min(40, tokens.length)).distinct
          }
          else List() 
      })
      .map(w => (w,1))
      .reduceByKey(_ + _)
      .sortByKey()
      .collectAsMap()
      // get a map of occurance of each words

      val pairs = textFile.flatMap(
        line => {
          val tokens = tokenize(line)
          if (tokens.length > 0){
            val wordsInOneLine= tokens.take(Math.min(40, tokens.length)).distinct
            // avoid pairs with two same words
            wordsInOneLine.flatMap(word1 => wordsInOneLine.map(word2 => (word1, word2))).filter(words => words._1 != words._2)
          }
          else List() 
      }).map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter(pair => (pair._2 >= threshold))
      .repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
      // .map(pair => (pair._1._1,pair._1._2,pair._2))
      // (left, right, count)

      // calculate PMI  Pair * numOfLines /(word_1 * word_2)
      // words.get(pair._1._1) gives Some(Int), convert it to int
      // ("a","b") \t (PMI, occurence)
      val pmis = pairs.map(
        pair => ( 
          pair._1,
          (
            Math.log10(
              pair._2.toFloat * numOfLines / (words.get(pair._1._1).get * words.get(pair._1._2).get)
            ),
            pair._2
          )
        )
      ).map(pmi => ("(" + pmi._1 +"\t"+pmi._2+")"))
      
      //compute PMI
      pmis.saveAsTextFile(args.output())

    }

  }

