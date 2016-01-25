package advanced.analytics
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
/***
 * 数据源
 * https://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html
 */
object Ch5_Kmeans {
	def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
	  val sc = new SparkContext(conf)
	  val rawData = sc.textFile("data/kdd/kddcup.data")
//	  val rawData = sc.textFile("data/kdd/kddcup.data_10_percent")
	  //rawData.first
	  //	  0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.
	  
	//   rawData.map(_.split(",").map(_.last)).countByValue.toSeq.sortBy(_._2).reverse.foreach(println)
	   rawData.map(_.split(",").last).countByValue.toSeq.sortBy(_._2).reverse.foreach(println)
	  //去除不是数值型的特征
	   
	   val labelAndData = rawData.map{line=>
	    val buffer = line.split(",").toBuffer
	    buffer.remove(1,3)
	    val label = buffer.remove(buffer.length-1)
	    val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
	    (label, vector)
       }.cache
      val data = labelAndData.map(_._2).cache
      val kmeans = new KMeans()
      val model = kmeans.run(data)
      model.clusterCenters.foreach(println)
	  
	  val clusterLabelCount = labelAndData.map{case (label,datum)=>
	  	val cluster = model.predict(datum)
	  	(cluster,label)
      }.countByValue
      
      clusterLabelCount.toSeq.sorted.foreach{
        case((cluster,label), count) =>
          println(f"$cluster%1s$label%18s$count%8s")
      }
      def distance(a:Vector, b :Vector)=
        math.sqrt(a.toArray.zip(b.toArray).map(p=> p._1 - p._2).map(d=>d*d).sum)
        
      def distToCentroid(datum:Vector,model:KMeansModel)={
        val cluster = model.predict(datum)
        val centroid = model.clusterCenters(cluster)
        distance(centroid,datum)
      }  
      
      def cluteringScore(data:RDD[Vector], k:Int) = {
        val kmeans = new KMeans()
        kmeans.setK(k)
        val model = kmeans.run(data)
        data.map(distToCentroid(_, model)).mean()
      }
      (5 to 40 by 5).map(k=> (k,cluteringScore(data,k))).foreach(println)
      
      
      val dataAsArray =  data.map(_.toArray)
      val numCols = dataAsArray.first.length//38
      val n = dataAsArray.count
//      n: Long = 494021 
      val sums = dataAsArray.reduce((a,b)=> a.zip(b).map(t=>t._1+t._2))
      val sumSquares = dataAsArray.fold(new Array[Double](numCols))((a,b)=>a.zip(b).map(t=>t._1+t._2*t._2))
      val stdevs = sumSquares.zip(sums).map{
        case(sumSq,sum) => math.sqrt(n* sumSq- sum*sum)/n
      }
      val means = sums.map(_/n)
      def normalize(datum: Vector) = {
        val normalizedArray = (datum.toArray, means,stdevs).zipped.map{
          (value,mean,stdev) =>
            if(stdev<=0)(value-mean) else (value-mean)/stdev
        }
        Vectors.dense(normalizedArray)
      }
      val normalizedData = data.map(normalize).cache
      (60 to 120 by 10).par.map(k=>
        (k,cluteringScore(normalizedData,k))).toList.foreach(println)
      
      
	
	}
}