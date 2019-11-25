package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import scala.math.exp
import org.apache.hadoop.fs._


class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    // The --input option specifies the input training instances (from above); the --model option specifies the output directory where the model goes.
    mainOptions = Seq(input)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model", required = true)
    val method = opt[String](descr = "method", required = true)
    verify()
}


object ApplyEnsembleSpamClassifier extends {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("model: " + args.model())
        log.info("method: " + args.method())

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

        // weight for x y and britney
        val weight_x = sc.textFile(args.model() + "/part-00000")
			.map(line => {
                // remove parentheses
                val instance = line.replaceAll("\\(","").replaceAll("\\)","").split(',')
				(instance(0).toInt, instance(1).toDouble)
			})
            .collectAsMap()
        val weightBroadcast_x = sc.broadcast(weight_x)

        val weight_y = sc.textFile(args.model() + "/part-00001")
			.map(line => {
                // remove parentheses
                val instance = line.replaceAll("\\(","").replaceAll("\\)","").split(',')
				(instance(0).toInt, instance(1).toDouble)
			})
            .collectAsMap()
        val weightBroadcast_y = sc.broadcast(weight_y)

        val weight_b = sc.textFile(args.model() + "/part-00002")
			.map(line => {
                // remove parentheses
                val instance = line.replaceAll("\\(","").replaceAll("\\)","").split(',')
				(instance(0).toInt, instance(1).toDouble)
			})
            .collectAsMap()
        val weightBroadcast_b = sc.broadcast(weight_b)

        // println(weight)

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int], weight: scala.collection.Map[Int,Double]) : Double = {
			var score = 0d
			features.foreach(f => if (weight.contains(f)) score += weight(f))
			score
		}


        // two techniques: Score averaging and voting
        val method = args.method()



        val judgement = inputTextFile.map(line => 
            {
                val instance = line.split(" ")
                val docid = instance(0)
                val isSpam = instance(1)
                val features = instance.drop(2).map(_.toInt)
                val score_x = spamminess(features, weightBroadcast_x.value)
                val score_y = spamminess(features, weightBroadcast_y.value)
                val score_b = spamminess(features, weightBroadcast_b.value)
                
                var score = 0d
                if (method == "average") {
				    score = (score_x + score_y + score_b) / 3   
                } else {
                    score = if(score_x>0) score + 1 else score-1
                    score = if(score_y>0) score + 1 else score-1
                    score = if(score_b>0) score + 1 else score-1
                }
                var label = if (score > 0) "spam" else "ham"
                (docid, isSpam, score, label)
            })
        judgement.saveAsTextFile(args.output())
    }
}

