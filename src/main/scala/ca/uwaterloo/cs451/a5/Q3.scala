package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}
// select l_orderkey, p_name, s_name from lineitem, part, supplier
// where
//   l_partkey = p_partkey and
//   l_suppkey = s_suppkey and
//   l_shipdate = 'YYYY-MM-DD'
// order by l_orderkey asc limit 20;
// lineItem[1] = part[0]
// lineItem[2] = supplier[0]
// show p_name part[1] and s_name = supplier[1]
object Q3{
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]) {
		val args = new ConfQ3(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())
        log.info("Input: " + args.text())
		log.info("Date: " + args.parquet())

		val conf = new SparkConf().setAppName("Q3")
		val sc = new SparkContext(conf)
		val date = args.date()
        
        if(args.text()){
            // read textFile
            val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
            val partTextFile = sc.textFile(args.input() + "/part.tbl")
            val supplierTextFile = sc.textFile(args.input() + "/supplier.tbl")
            val lineItems = lineItemTextFile
                // (l_orderkey, l_partkey, l_suppkey, l_shipdate)
                .map(line => (line.split('|')(0),line.split('|')(1),line.split('|')(2),line.split('|')(10)))
                .filter(_._4.contains(date))
            
            val part = partTextFile
                // (partkey, p_name)
                .map(line =>(line.split('|')(0).toInt, line.split('|')(1)))
            val supplier = supplierTextFile
                // (supplierKey, s_name)
                .map(line =>(line.split('|')(0).toInt, line.split('|')(1)))

            // RDD.collect Return an array that contains all of the elements in this RDD.
            val partBroadcast = sc.broadcast(part.collectAsMap())
            val supplierBroadcast = sc.broadcast(supplier.collectAsMap())

            val partMap = partBroadcast.value
            val supplierMap = supplierBroadcast.value
            
            //Foreach is an action, it takes each element and applies a function, but it does NOT return a value.
            val records = lineItems
                .map(lineItem =>{
                    // val partMap = partBroadcast.value
                    // val supplierMap = supplierBroadcast.value
        
                    val l_ORDERKEY = lineItem._1.toInt
                    val l_PARTKEY = lineItem._2.toInt
                    val l_SUPPKEY = lineItem._3.toInt
                    val l_SHIPDATE = lineItem._4
                    // println(l_ORDERKEY, l_PARTKEY, l_SUPPKEY, l_SHIPDATE)
                    val p_name = partMap(l_PARTKEY)
                    val s_name = supplierMap(l_SUPPKEY)
                    // println(l_ORDERKEY,p_name,s_name)
                    (l_ORDERKEY,(p_name,s_name))
                    }
                )
                .sortByKey()
                .take(20)
                .foreach(pair=>println(pair._1, pair._2._1, pair._2._2))
        }
        else if(args.parquet()){
            val sparkSession = SparkSession.builder.getOrCreate
            val lineItemFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val partFileDF = sparkSession.read.parquet(args.input() + "/part")
            val supplierFileDF = sparkSession.read.parquet(args.input() + "/supplier")
            
            val lineitemRdd = lineItemFileDF.rdd
            val partRdd = partFileDF.rdd
            val supplierRdd = supplierFileDF.rdd

            val part = partRdd
                // (partkey, p_name)
                .map(line =>(line.getInt(0), line.getString(1)))
            val supplier = supplierRdd
                // (supplierKey, s_name)
                .map(line =>(line.getInt(0), line.getString(1)))

            val partBroadcast = sc.broadcast(part.collectAsMap())
            val supplierBroadcast = sc.broadcast(supplier.collectAsMap())

            val partMap = partBroadcast.value
            val supplierMap = supplierBroadcast.value

            val lineItem = lineitemRdd
                .filter(line=>line.getString(10).contains(date))
                .map(line =>{
                    val l_ORDERKEY = line.getInt(0)
                    val l_PARTKEY = line.getInt(1)
                    val l_SUPPKEY = line.getInt(2)
                    // val l_SHIPDATE = lineItem.getString(3)
                    // println(l_ORDERKEY, l_PARTKEY, l_SUPPKEY, l_SHIPDATE)
                    val p_name = partMap(l_PARTKEY)
                    val s_name = supplierMap(l_SUPPKEY)
                    // println(l_ORDERKEY,p_name,s_name)
                    (l_ORDERKEY,(p_name,s_name))
                    }
                )
                .sortByKey()
                .take(20)
                .foreach(pair=>println(pair._1, pair._2._1, pair._2._2))
        }
    }
}