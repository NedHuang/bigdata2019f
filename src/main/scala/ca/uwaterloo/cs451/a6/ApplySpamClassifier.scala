package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.exp
import org.apache.hadoop.fs._


class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    // The --input option specifies the input training instances (from above); the --model option specifies the output directory where the model goes.
    mainOptions = Seq(input)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model", required = true)
    verify()
}


object ApplySpamClassifier extends {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ApplySpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("model: " + args.model())

        // Conf and SparkContext
        val conf = new SparkConf().setAppName("ApplySpamClassifier")
        val sc = new SparkContext(conf)
        // delete outPutPath if it exists.
        val outPutPath = new Path(args.output())
        val fs = FileSystem.get(sc.hadoopConfiguration)
		if (fs.exists(outPutPath)){
            fs.delete(outPutPath, true)
        }
        

        var inputTextFile = sc.textFile(args.input())
        // val inputdata = inputTextFile.map(line=>{
        //         val instance = line.split(" ")
        //         val docid = instance(0)
        //         val isSpam = instance(1)
        //         val features = instance.drop(2).map(_.toInt)
        //         (docid,isSpam)
        //     }
        // )
        
        // val judgement = inputTextFile.map(line=>{
        //         val instance = line.split(" ")
        //         val docid = instance(0)
        //         val isSpam = instance(1)
        //         val features = instance.drop(2).map(_.toInt)
        //         (0, (docid, isSpam, features))
        //     }
        // )

        val weight = sc.textFile(args.model() + "/part-00000")
			.map(line => {
                // remove parentheses
                val instance = line.replaceAll("\\(","").replaceAll("\\)","").split(',')
				(instance(0).toInt, instance(1).toDouble)
			})
            .collectAsMap()
        val weightBroadcast = sc.broadcast(weight)
        // println(weight)

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
			var score = 0d
			features.foreach(f => if (weightBroadcast.value.contains(f)) score += weightBroadcast.value(f))
			score
		}


        val judgement = inputTextFile.map(line => 
            {
                val instance = line.split(" ")
                val docid = instance(0)
                val isSpam = instance(1)
                val features = instance.drop(2).map(_.toInt)
                val score = spamminess(features)
                val label = if (score > 0) "spam" else "ham"
                (docid, isSpam, score, label)
            })
        judgement.saveAsTextFile(args.output())
    }
}

