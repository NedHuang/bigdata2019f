package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = false)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}

object Q1 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new ConfQ1(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

		val conf = new SparkConf().setAppName("Q1")
		val sc = new SparkContext(conf)

		val date = args.date()
    if (args.text()) {
      val lines = sc.textFile(args.input() + "/lineitem.tbl")
      val count = lines
      .map(line => line.split("\\|")(10))
      .filter(_.contains(date))
      .count
      println("ANSWER=" + count)
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      // The result of loading a Parquet file is also a DataFrame
      val parquetFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
      
      val lineitemRDD = parquetFileDF.rdd
  		val count = lineitemRDD
  			.map(line => line.getString(10))
  			.filter(_.contains(date))
  			.count
  		println("ANSWER=" + count)
    }
	}
}