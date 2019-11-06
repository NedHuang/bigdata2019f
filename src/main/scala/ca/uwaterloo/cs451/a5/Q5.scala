package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
//   val date = opt[String](descr = "date", required = false)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}
/*
compare shipments to Canada vs. the United States by month
*/
object Q5 extends {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ConfQ5(argv)

        log.info("Input: " + args.input())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("Q5")
        val sc = new SparkContext(conf)

        if (args.text()) {
            val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
            val customerTextFile = sc.textFile(args.input() + "/customer.tbl")
            val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
            val nationTextFile = sc.textFile(args.input() + "/nation.tbl")

            //N_NATIONKEY, N_NAME for US and Canada
            // val canada = "CANADA"
            // val us = "UNITED STATES"
            // val canada_nKey = "3"
            // val us_nKey = "24"

            val lineItems = lineItemTextFile
                // (l_ORDERKEY, date in YYYY-MM)
                .map(line =>(line.split('|')(0).toInt, line.split('|')(10).substring(0,7)))
                
            val customer = customerTextFile
                // (C_CUSTKEY, C_NATIONKEY)
                .map(line=>(line.split('|')(0).toInt, line.split('|')(3).toInt))

            val nation = nationTextFile
                // (N_NATIONKEY, n_name)
                .map(line =>(line.split('|')(0).toInt, line.split('|')(1)))

            val customerBroadcast = sc.broadcast(customer.collectAsMap())
            val nationBroadcast = sc.broadcast(nation.collectAsMap())

            // val lineItemsMap = lineItemsBroadcast.value
            val customerMap = customerBroadcast.value
            val nationMap = nationBroadcast.value
            
            // this works
            // val orders = ordersTextFile
            //     // (O_ORDERKEY, O_CUSTKEY)
            //     .filter(
            //         line=>{

            //             val nationkey = customerMap(line.split('|')(1).toInt)
            //             nationkey == 3 || nationkey == 24
            //         }
            //     )
            //     .map(line=>(line.split('|')(0).toInt, customerMap(line.split('|')(1).toInt)))

            //     .foreach(println)

            val orders = ordersTextFile
                // (O_ORDERKEY, O_CUSTKEY)
                .map(line=>(line.split('|')(0).toInt, line.split('|')(1).toInt))

            // O_ORDERKEY, NATIONKEY
            val order_with_nationKey = orders
                .filter(
                    pair=>{ customerMap(pair._2) == 3 || customerMap(pair._2) == 24 }
                )
                .map(
                    pair=>(pair._1.toInt, customerMap(pair._2))
                )
            //(L_ORDERKEY, (date, NATIONKEY))
            val recorder = lineItems
                .cogroup(order_with_nationKey) 
                .filter(pair => pair._2._2.iterator.hasNext)
                .flatMap(pair => {
                    val nationKey = pair._2._2
                    pair._2._1.map(date => ((nationKey, date), 1))
                })
                .reduceByKey(_+_)
                .sortByKey()
                .collect()
                .foreach(pair => println(pair._1._1.mkString(""), pair._1._2, pair._2))
        }
        else if(args.parquet()){
            val sparkSession = SparkSession.builder.getOrCreate
            val lineItemFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val customerFileDF = sparkSession.read.parquet(args.input() + "/customer")
            val ordersFileDF = sparkSession.read.parquet(args.input() + "/orders")
            val nationFileDF = sparkSession.read.parquet(args.input() + "/nation")

            val lineItemRdd = lineItemFileDF.rdd
            val customerRdd = customerFileDF.rdd
            val ordersRdd = ordersFileDF.rdd
            val nationRdd = nationFileDF.rdd


            val lineItems = lineItemRdd
                // (l_ORDERKEY, date in YYYY-MM)
                .map(line =>(line.getInt(0), line.getString(10).substring(0,7)))
               
            val customer = customerRdd
                // (C_CUSTKEY, C_NATIONKEY)
                .map(line=>(line.getInt(0), line.getInt(3)))

            val nation = nationRdd
                // (N_NATIONKEY, n_name)
                .map(line =>(line.getInt(0), line.getString(1)))

            val customerBroadcast = sc.broadcast(customer.collectAsMap())
            val nationBroadcast = sc.broadcast(nation.collectAsMap())

            // val lineItemsMap = lineItemsBroadcast.value
            val customerMap = customerBroadcast.value
            val nationMap = nationBroadcast.value
            

            val orders = ordersRdd
                // (O_ORDERKEY, O_CUSTKEY)
                .map(line=>(line.getInt(0), line.getInt(1)))

            // O_ORDERKEY, NATIONKEY
            val order_with_nationKey = orders
                .filter(
                    pair=>{ customerMap(pair._2) == 3 || customerMap(pair._2) == 24 }
                )
                .map(
                    pair=>(pair._1.toInt, customerMap(pair._2))
                )
            //(L_ORDERKEY, (date, NATIONKEY))
            val recorder = lineItems
                .cogroup(order_with_nationKey) 
                .filter(pair => pair._2._2.iterator.hasNext)
                .flatMap(pair => {
                    val nationKey = pair._2._2
                    pair._2._1.map(date => ((nationKey, date), 1))
                })
                .reduceByKey(_+_)
                .sortByKey()
                .collect()
                .foreach(pair => println(pair._1._1.mkString(""), pair._1._2, pair._2))
        }
    }
}