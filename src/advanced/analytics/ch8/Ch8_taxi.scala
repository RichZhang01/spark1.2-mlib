package advanced.analytics.ch8

import com.github.nscala_time.time.Imports.DateTime
import com.github.nscala_time.time.Imports.Duration
import com.github.nscala_time.time.Imports.richDateTime
import com.github.nscala_time.time.Imports.richInt
import com.github.nscala_time.time.Imports.richReadableInstant
import java.text.SimpleDateFormat
import spray.json.JsValue
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spray.json.RootJsonFormat
import spray.json.JsString
import spray.json.JsObject
import com.esri.core.geometry.Point
/***
 * spark-shell --master spark://dchdmaster2:7077 --executor-memory 10g --driver-memory 10g --jars esri-geometry-api-1.2.1.jar,nscala-time_2.10-2.6.0.jar,spray-json_2.10-1.3.2.jar
 */
object Ch8_taxi {
	def main(args: Array[String]) {
	  
	      val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
  val sc = new SparkContext(conf)
  
  
		import com.github.nscala_time.time.Imports._
		val dt1 = new DateTime(2014,9,4,9,0);
		dt1.dayOfYear().get()
		//247
		val dt2 = new DateTime(2014,10,31,15,0)
		dt1<dt2
		val dt3 = dt1+  60.days
		import java.text.SimpleDateFormat
		val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		val date = format.parse("2014-10-12 10:30:44")
		val datetime = new DateTime(date)
		val d = new Duration(dt1,dt2)
		d.getMillis()
		d.getStandardHours()
		d.getStandardDays()
		

		
		import com.esri.core.geometry.Point
		import com.github.nscala_time.time.Imports._
		case class Trip(pickupTime:DateTime,
		    dropoffTime:DateTime,
		    pickupLoc:Point,dropoffLoc:Point)
		val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		
		def point(longitude:String, latitude:String):Point={
		  new Point(longitude.toDouble,latitude.toDouble)
		}
		def parse(line:String):(String,Trip)={
		  val fields = line.split(",")
		  val licence = fields(1)
		  val pickupTime = new DateTime(formatter.parse(fields(5)))
		  val dropoffTime = new DateTime(formatter.parse(fields(6)))
		  val pickupLoc = point(fields(10),fields(11))
		  val dropoffLoc = point(fields(12),fields(13))
		  val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
		  (licence,trip)
		}
		
		 def safe[S,T](f:S=>T):   S => Either[T,(S,Exception)] = {
		    new Function[S,Either[T,(S,Exception)]] with Serializable{
		      def apply(s:S): Either[T,(S,Exception)] = {
		        try{
		          Left(f(s))
		        }catch{
		          case e:Exception => Right((s,e))
		        }
		      }
		    }
		  }
		 

		 
		 
		 
//		 safe: [S, T](f: S => T)S => Either[T,(S, Exception)]
		 
				 val taxiRaw = sc.textFile("data/trip_data_1.csv")
		 val taxiHead = taxiRaw.take(10)
		 taxiHead.foreach(println)
		 val safeParse = safe(parse)
		 val taxiParsed = taxiRaw.map(safeParse)
		 (false,87)                                                                      
(true,14776529)
		 taxiParsed.cache
		 taxiParsed.map(_.isLeft).countByValue.foreach(println)
		 
		 val taxiBad = taxiParsed.filter(_.isRight).map(_.right.get)
		 
		 val taxiBad2 = taxiParsed.collect({case t if t.isRight=>t.right.get})
		 
		 taxiBad.collect.foreach(println)
		 
		 
		 val taxiGood = taxiParsed.collect({case t if t.isLeft => t.left.get})
		 taxiGood.cache
		 
		
		 
		 def hours(trip:Trip):Long = {
		    val d = new Duration(trip.pickupTime,trip.dropoffTime)
		    d.getStandardHours()
	     }  
	    taxiGood.cache()
	    taxiGood.map(x=>hours(x._2)).countByValue.toList.sorted.foreach(println)
//	    (-8,1)                                                                          
//(0,14752245)
//(1,22933)
//(2,842)
//(3,197)
//(4,86)
//(5,55)
//(6,42)
//(7,33)
//(8,17)
//(9,9)
//(10,10)
//(11,13)
//(12,7)
//(13,5)
//(14,5)
//(15,3)
//(16,5)
//(17,3)
//(19,2)
//(20,2)
//(24,2)
//(25,1)
//(26,1)
//(28,1)
//(30,1)
//(51,1)
//(168,2)
//(336,1)
//(504,4)
	    taxiGood.filter(x=> hours(x._2)== -8).collect
//	    
	    val taxiClean = taxiGood.filter{
	      case(lic,trip)=> {
	        val hrs = hours(trip)
	        0 <=hrs && hrs<3
	      }
	      
	    }
	    taxiClean.map(x=> hours(x._2)).countByValue.toSeq.sortBy(_._1).foreach(println)
//	    
//	    val geojson = scala.io.Source.fromFile("/root/scout/nyc-borough-boundaries-polygon.geojson").mkString
	    		val geojson = scala.io.Source.fromFile("D:\\Documents\\Downloads\\nyc-borough-boundaries-polygon.geojson").mkString
	    import GeoJsonProtocol._
	    import spray.json._
	    val features = geojson.parseJson.convertTo[FeatureCollection]
	   //  geojson.parseJson//
	//    res1: spray.json.JsValue = {"type":"FeatureCollection","features":[{"type":"Feature","id":0,"properties":{"boroughCode":5,"borough":"Staten Island","@id":"http://nyc.pediacities.com/Resource/Borough/Staten_Island"},"geometry":{"type":"Polygon","coordinates":[[[-74.050508064032471,40.566422034160816],[-74.049983525625748,40.566395924928273],[-74.049316403620878,40.565887747780437],[-74.049236298420453,40.565362736368101],[-74.050026201586434,40.565318180621134],[-74.050906017050892,40.566094342130597],[-74.050679167486138,40.566310845736403],[-74.05107159803778,40.566722493397798],[-74.050508064032471,40.566422034160816]]]}},{"type":"Feature","id":1,"properties":{"boroughCode":5,"borough":"Staten Island","@id":"http://nyc.pediacities.com/Resource/Borough/Staten_Island"},"geometry":{"type...
	    
	    println(features)
	    
	    val p = new Point(-73.994499,40.75066)
	    val borough = features.find(f => f.geometry.contains(p))
	    val areaSortedFeatures = features.sortBy(f=> {
	      val borough = f("boroughCode").convertTo[Int]
	      (borough,-f.geometry.area2D)
	    })
	    
	    val bFeatures = sc.broadcast(areaSortedFeatures)
	    def borough2(trip:Trip): Option[String] = {
	      val feature:Option[Feature] = bFeatures.value.find(f=>{
	        f.geometry.contains(trip.dropoffLoc)
	      })
	      feature.map(f=>{
	        f("borough").convertTo[String]
	      })
	    }
//	    taxiClean.map(borough2)
	}
}