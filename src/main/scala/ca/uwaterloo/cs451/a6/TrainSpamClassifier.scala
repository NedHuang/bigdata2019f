package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.exp
import org.apache.hadoop.fs._


class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    // The --input option specifies the input training instances (from above); the --model option specifies the output directory where the model goes.
    mainOptions = Seq(input)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model", required = true)
    val shuffle = opt[Boolean](descr = "shuffle", required=false)
    verify()
}


object TrainSpamClassifier extends {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new TrainSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("model: " + args.model())

        // Conf and SparkContext
        val conf = new SparkConf().setAppName("TrainSpamClassifier")
        val sc = new SparkContext(conf)
        // delete outPutPath if it exists.
        val outPutPath = new Path(args.model())
        val fs = FileSystem.get(sc.hadoopConfiguration)
		if (fs.exists(outPutPath)){
            fs.delete(outPutPath, true)
        }

        var inputTextFile = sc.textFile(args.input())
        // w is the weight vector, it should be amutable map
        val w = scala.collection.mutable.Map[Int,Double]()

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]) : Double = {
            var score = 0d
            features.foreach(f => if (w.contains(f)) score += w(f))
            score
        }

        // This is the main learner:
        val delta = 0.002

        // shuffle or not...
        if(args.shuffle()){
            // textFile = Random.shuffle(inputTextFile)
            inputTextFile = inputTextFile
				.map(line => (scala.util.Random.nextInt(), line))
				.sortByKey()
				.map(p => p._2)
        }

        // for each instance: docid, isSpam, features
        // clueweb09-en0094-20-13546 spam 387908 697162 426572 161118 688171 ...
        val trained = inputTextFile
            .map(line =>{
                val instance = line.split(" ")
                val docid = instance(0)
                val isSpam = if (instance(1) == "spam") 1d else 0d
                val features = instance.drop(2).map(_.toInt)
                (0, (docid, isSpam, features))
            })
            .groupByKey(1)
            .flatMap(pair => {
                pair._2.foreach(instance => {
                    val docid = instance._1
                    val isSpam = instance._2
                    val features = instance._3
                    // Update the weights as follows:
                    val score = spamminess(features)
                    val prob = 1.0 / (1 + exp(-score))
                    features.foreach(f => {
                        if (w.contains(f)) {
                            w(f) += (isSpam - prob) * delta
                        } else {
                            w(f) = (isSpam - prob) * delta
                        }
                    })
                })
                w
            })
       trained.saveAsTextFile(args.model())
    }
}

