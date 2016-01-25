package advanced.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Ch1Demo2 extends App{
  val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
  val sc = new SparkContext(conf)
  val rawblocks = sc.textFile("data/linage")
  
  def isHeader(line:String) = line.contains("id_1")
  val head = rawblocks.take(10)
  head.filterNot(isHeader)
  val parsed = rawblocks.filter(!isHeader(_)).map(parse)
   import org.apache.spark.util.StatCounter
 val scores = parsed.map(_.scores(1)).stats
 import java.lang.Double.isNaN
 (0 until 9).map(i => (i, parsed.map(_.scores(i)).filter(!isNaN(_)).stats)).foreach(println)   
 
 
 
case class MatchData(id1:Int,id2:Int, scores:Array[Double], matched:Boolean)
def toDouble(s:String)={
	  if("?".equals(s)) Double.NaN else s.toDouble
  }
  def parse(line:String) = {
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2,11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1,id2,scores,matched)
  }
  

  import org.apache.spark.util.StatCounter
  
  class NAStatCounter2 extends Serializable{
    
    val stats :StatCounter = new StatCounter
    
    var missing :Long = 0
    def add(x:Double) : NAStatCounter2 ={
      if(java.lang.Double.isNaN(x)) {
        missing +=1
      }else{
        stats.merge(x)
      }
      this
    }
    def merge(other:NAStatCounter2):NAStatCounter2={
      stats.merge(other.stats)
      this
    }
    override def toString = {
      "stat :" +stats.toString +" NaN:" +missing
    }
  }
  object NAStatCounter2 extends Serializable{
    def apply(x:Double) = new NAStatCounter2().add(x)
  }
  
  
  val nas1 =  NAStatCounter2(10.0)
  val nas2 =  NAStatCounter2(Double.NaN)
  val arr = Array(1.0,Double.NaN,17.29)
  val nas = arr.map(NAStatCounter2(_))
  
  
  val nas11 = Array(1.0,Double.NaN).map(NAStatCounter2(_))
  val nas22 = Array(Double.NaN, 2.0).map(NAStatCounter2(_))
  val merged = nas11.zip(nas22)
  merged.map{case (a,b)=> a.merge(b)}
  
  
  val nas33 = List(nas11,nas22)
  val merged33 = nas33.reduce((n1,n2)=>{
    n1.zip(n2).map{case (a,b) => a.merge(b)}
  })
  
  
  val nas44 = List(nas11,nas22)
  val merge44 =  nas44.reduce((n1,n2)=>{
     n1.zip(n2).map{case (a,b)=>a.merge(b)}
  })
  
  val nasRDD = parsed.map(_.scores.map(NAStatCounter2(_)))
  
  nasRDD.reduce((n1,n2)=>
  	n1.zip(n2).map{case (a,b)=> a.merge(b)}
  )
  
  import org.apache.spark.rdd.RDD
  def statWithMissing(rdd:RDD[Array[Double]]) : Array[NAStatCounter2]={
    val nastats = rdd.mapPartitions({iter:Iterator[Array[Double]] =>
    val nas : Array[NAStatCounter2] = iter.next().map(NAStatCounter2(_))
    iter.foreach(arr=>{
    	nas.zip(arr).foreach{case (n,d)=>n.add(d)}
    })
    Iterator(nas)
    })
    nastats.reduce((n1,n2)=>{
      n1.zip(n2).map{case (a,b)=> a.merge(b)}
    })
  }
  

  
  
  
  
  
  
  
  
}