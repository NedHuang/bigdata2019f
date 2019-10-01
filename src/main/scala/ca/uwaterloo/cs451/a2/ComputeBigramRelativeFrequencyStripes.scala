 
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
  class Conf2(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, reducers)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "executor-cores", required = true, default = Some(2))
    val numExecutors = opt[Int](descr = "num-executors", required = true, default = Some(4))
    val executorMemory = opt[String](descr = "executor-memory", required = true, default = Some("24G"))
    verify()
  }
  
  class StripePartitioner(partitions: Int) extends Partitioner {
    def numPartitions: Int = partitions
    def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[(String, String)]
      ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
    }
  }

  object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
    val log = Logger.getLogger(getClass().getName())
  
    def main(argv: Array[String]) {
      print("Hello World")
      val args = new Conf2(argv)
  
      log.info("Input: " + args.input())
      log.info("Output: " + args.output())
      log.info("Number of reducers: " + args.reducers())
  
      val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
      val sc = new SparkContext(conf)
  
      val outputDir = new Path(args.output())
      FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

      var sum = 0.0f
      val textFile = sc.textFile(args.input())

      // flatMap: take a function as input, apply it into vars, return a set of result to each var. Finally aggregate them into a big set
      val counts = textFile.flatMap(line => {
        val tokens = tokenize(line)
        // assume tokens = List('my', 'hero', 'academia') => list( list('my','hero'), list('hero','academia'))   \\ sliding
        if (tokens.length > 1){
          tokens.sliding(2).flatMap(
            //  ((my ,hero),(my,*)), ((hero,academia),(hero,*)))
            pair => List((pair(0),pair(1)), (pair(0), "*"))).toList
        }
        else List() 
      }) // now get many lists of (p(0),p(1)), (p(0), '*')
      .map(tempResult => (tempResult, 1)) // add the count to each list of (p(0),p(1)), (p(0), '*'), e.g., List((((my,hero),(my,*)),1), (((hero,academia),(hero,*)),1))
      // same as  reduceByKey((x,y)=> x + y)
      .reduceByKey(_ + _)
      // .repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
      /*
        temp output need to be sorted..
        ((anger,with),1)
        ((off,myself),1)
        ((with,cannibals),1)
        ((our,heels),1)
        ((night,she),3)
        ((all,matter),1)
        ((faithful,services),1)
        ((pretty,lad),1)
        ((ten,days),4)
        ((cost,of),3)
        ((smooths,*),1)
        ((precisely,can),1)
        ((northumberlands,two),1)
        ((your,fashion),1)
        
      */
      // now deal with data using strips
    .map(pair => pair._1._2 match {
      case "*"  => {
        sum = pair._2
        (pair._1, pair._2)
      }
      case _ => (pair._1, pair._2 / sum)
    })
    // remove something like (('aaaa','*'),1)
    .filter((pair) => pair._1._2 != "*")
    // do stripes , turn ((a,b), 0.5), ((a,c),0.5)  to (a, ((b, 0.5),(c, 0.5)) ) ...
    .map(
      pair => (pair._1._1, (pair._1._2 + "="+ pair._2.toString)) //  (a, ((b, 0.5)),   (a, ((c, 0.5))... need to group by key
    )
    .groupByKey()
    
    /*
      (saddest,CompactBuffer(spectacle,0.5, tale,0.5))
      (painfully,CompactBuffer(discover'd,0.25, remain,0.25, to,0.25, with,0.25))
      (defendant,CompactBuffer(and,0.6666667, doth,0.33333334))
      (cricket,CompactBuffer(to,1.0))
      need to be sorted..
      need to split the stripes into pairs
    */
    .map(s => (s._1, s._2.toList.mkString(", ")))
    counts.map(s => '(' + s._1 + ",{" + s._2 + "})").saveAsTextFile(args.output())
    }
  }

