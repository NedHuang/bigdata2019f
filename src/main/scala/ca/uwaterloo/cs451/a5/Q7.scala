package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = false)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}
/*
select
  c_name,
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from customer, orders, lineitem
where
  c_custkey = o_custkey and
  l_orderkey = o_orderkey and
  o_orderdate < "YYYY-MM-DD" and
  l_shipdate > "YYYY-MM-DD"
group by
  c_name,
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc
limit 10;
*/

object Q7 extends {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ConfQ7(argv)
        
        log.info("Input: " + args.input())
		log.info("Date: " + args.date())
        log.info("Input: " + args.text())
		log.info("Date: " + args.parquet())

        val date = args.date()
        val conf = new SparkConf().setAppName("Q7")
        val sc = new SparkContext(conf)

        if (args.text()) {
            val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
            val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
            val customerTextFile = sc.textFile(args.input() + "/customer.tbl")

            val customer = customerTextFile
            // (C_CUSTKEY, C_NAME)
                .map(line => {
                (line.split('|')(0).toInt, line.split('|')(1))
                })
                .collectAsMap()

            val customerBroadcast = sc.broadcast(customer)

            val orders = ordersTextFile
                .filter(line => {
                line.split('|')(4) < date
                })
                .map(line => {
                val tokens = line.split('|')
                // (O_ORDERKEY,(O_CUSTKEY,O_ORDERDATE,O_SHIPPRIORITY))
                (tokens(0), (tokens(1), tokens(4), tokens(7)))
                })

            val lineItem = lineItemTextFile
                .filter(line => {
                    line.split('|')(10) > date
                })
                .map(line => {
                val tokens = line.split('|')
                val price = tokens(5).toDouble
                val discount = tokens(6).toDouble
                val revenue = price * (1.0 - discount)
                // l_ORDERKEY, revenue
                (tokens(0), revenue)
                })
                .reduceByKey(_+_)

            lineItem.cogroup(orders)
                .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
                .map(
                    p => {
                    val tuple = p._2._2.iterator.next()
                    (
                        (customerBroadcast.value(tuple._1.toInt), p._1, tuple._2, tuple._3),
                        p._2._1.iterator.next()
                    )
                })
                .sortBy(- _._2)
                .take(10)
                .foreach(
                    p => 
                    println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + "," + p._1._3 + "," + p._1._4 + ")")
                )

        } 
        else if(args.parquet()){
            val sparkSession = SparkSession.builder.getOrCreate
            val lineItemFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val customerFileDF = sparkSession.read.parquet(args.input() + "/customer")
            val ordersFileDF = sparkSession.read.parquet(args.input() + "/orders")

            val lineItemRdd = lineItemFileDF.rdd
            val customerRdd = customerFileDF.rdd
            val ordersRdd = ordersFileDF.rdd

            val custkeys = customerRdd
                .map(line => (line(0), line(1)))
                .collectAsMap()
            val customerBroadcast = sc.broadcast(custkeys)

            val orders = ordersRdd
                .filter(line => line(4).toString < date)
                // (O_ORDERKEY,(O_CUSTKEY,O_ORDERDATE,O_SHIPPRIORITY))
                .map(line => (line(0), (line(1), line(4), line(7))))

            val lineItem = lineItemRdd
                .filter(line => line(10).toString > date)
                .map(
                    line => {
                        val price = line(5).toString.toDouble
                        val discount = line(6).toString.toDouble
                        val revenue = price * (1.0 - discount)
                        (line(0), revenue)
                    }
                )
                .reduceByKey(_+_)

            lineItem.cogroup(orders)
                .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
                .map(p => {
                    val tuple = p._2._2.iterator.next()
                    ( (customerBroadcast.value(tuple._1), p._1, tuple._2, tuple._3),
                    p._2._1.iterator.next() )
                })
                .sortBy(- _._2)
                .take(10)
                .foreach(
                    p => 
                    {println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + "," + p._1._3 + "," + p._1._4 + ")")}
                )
        }
    }
}
