package advanced.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Arrays

object Ch1Demo extends App{
	import org.apache.spark.SparkContext._
  def toDouble(s:String) ={
    if("?".equals(s))Double.NaN else s.toDouble
  }
  def parse2(line:String) = {
	  val pieces = line.split(",")
			  val id1 = pieces(0).toInt
			  val id2 = pieces(1).toInt
			  val scores = pieces.slice(2,11).map(toDouble)
			  val matched = pieces(11).toBoolean
			  (id1,id2,scores,matched)
  }
  def parse(line:String) = {
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2,11).map(toDouble)
    val matched = pieces(11).toBoolean
    //(id1,id2,scores,matched)
    MatchData(id1,id2,scores,matched)
  }
  val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
  val sc = new SparkContext(conf)
  val data = sc.textFile("data/linage")
  val head = data.take(10)
 def isHeader(line:String):Boolean = line.contains("id")
 head.filter(isHeader).foreach(println)
 val line = head(5)
  val tup = parse2(line)
  tup._1
  tup.productArity  //??
  tup.productElement(0)
  case class MatchData(id1:Int,id2:Int,scores:Array[Double],matched:Boolean)
  
  val mds = head.filter(x=> !isHeader(x)).map(x=>parse(x))
  
  
  val noheader = data.filter(!isHeader(_))
  val parsed = noheader.map(line=>parse(line))
  parsed.cache
  val grouped = mds.groupBy(_.matched)
  grouped.mapValues(x=>x.size).foreach(println)
  
  val matchCounts = parsed.map(_.matched).countByValue
  val matchCoutsSeq = matchCounts.toSeq
  matchCoutsSeq.sortBy(_._1)
  
  parsed.map(_.scores(0)).stats
  
  parsed.map(_.scores).foreach(x=>println(Arrays.toString(x)))
  
    parsed.map(_.scores(2)).stats
//    res27: org.apache.spark.util.StatCounter = (count: 20, mean: NaN, stdev: NaN, max: NaN, min: NaN)
  
    import java.lang.Double.isNaN
    parsed.map(_.scores(2)).filter(!isNaN(_)).stats
    
    val stats = (0 until 9).map(i=>parsed.map(_.scores(i)).filter(!isNaN(_)).stats)
    
    
    import org.apache.spark.util.StatCounter
    class NAStatCounter extends Serializable{
		val stats: StatCounter = new StatCounter
		var missing:Long =0
		def add(x:Double):NAStatCounter = {
		  if(java.lang.Double.isNaN(x)){
		    missing+=1
		  }else{
		    stats.merge(x)
		  }
		  this
		}
		def merge(other:NAStatCounter):NAStatCounter = {
		  stats.merge(other.stats)
		  missing +=other.missing
		  this
		}
		
		override def toString ={
		  "stat: " +stats.toString +"  NaN:" +missing
		}
	}
    object NAStatCounter extends Serializable{
      def apply(x:Double) = new NAStatCounter().add(x)
    }
    
    
    
    val nstats = NAStatCounter(17.29)
    
    val nas1 = NAStatCounter(10.0)
    nas1.add(2.1)
    val na2 = NAStatCounter(Double.NaN)
    
}