package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}


object Q2 {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new ConfQ2(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())
        log.info("Input: " + args.text())
		log.info("Date: " + args.parquet())
		val conf = new SparkConf().setAppName("Q2")
		val sc = new SparkContext(conf)
		val date = args.date()
        
        if(args.text()){
            // read textFile
            val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
            val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
            // (O_ORDERKEY, o_clerk)
            val orders = ordersTextFile
                .map(line=>(line.split('|')(0), line.split('|')(6)))

            // have orderKey and date. return 
            val lineItem =lineItemTextFile
                // (O_ORDERKEY, date) O_ORDERKEY is a foreign key to order key
                // // .map(line=>(line.split('|')(0), line.split('|')(10)))
                // // .filter(_._2.contains(date))
                .filter(line=>line.split('|')(10).contains(date))

            // val records = lineItem
            // // combine  (K, V) and (K, W) and returns a dataset of (K, (Iterable, Iterable)) tuples
            // // (key, (CompactBuffer(),CompactBuffer(Clerk#)))
            //     .cogroup(orders)
            //     .filter(_._2._1.size != 0) // remove if orderKey is empty
            //     .sortByKey()
            //     // List the first 20 by order key
            //     .take(20)
            //     // get o_clerk from compactbuffer // cast O_ORDERKEY to Long, 
            //     .map(record => (record._2._2.head, record._1.toLong))
            //     .foreach(println)
            val records = lineItem
                // (OrderKey,1)
                .map(line=>(line.split('|')(0),1))
                .cogroup(orders)
                .filter(p => p._2._1.iterator.hasNext)
                .map(line => (line._1.toInt, line._2._2.mkString))
                .sortByKey()
                .take(20)
                .map(line => (line._2, line._1))
                .foreach(println) 
        }
        else if(args.parquet()){
            val sparkSession = SparkSession.builder.getOrCreate
            val lineItemFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val ordersFileDF = sparkSession.read.parquet(args.input() + "/orders")
            
            val lineItemRdd = lineItemFileDF.rdd
            val ordersRdd = ordersFileDF.rdd

            val orders = ordersRdd
                .map(line=>(line.getInt(0), line.getString(6)))
            val lineItems =lineItemRdd
                .map(line=>(line.getInt(0), line.getString(10)))
                .filter(_._2.contains(date))
            val records = lineItems
                .cogroup(orders)
                .filter(_._2._1.size != 0) 
                .sortByKey()
                .take(20)
                .map(record => (record._2._2.head, record._1.toLong))
                .foreach(println)

        }
    }
}
