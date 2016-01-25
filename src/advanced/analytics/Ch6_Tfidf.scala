package advanced.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Ch6_Tfidf {
	def main(args: Array[String]) {
	   val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
	  val sc = new SparkContext(conf)
	   val path = ""
	  val rawblocks = sc.textFile(path)
	  
	  
	  
	  
	}
}