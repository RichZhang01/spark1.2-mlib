package advanced.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.MulticlassMetrics


object Ch4DecesionTreeDemo extends App{

    val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
  val sc = new SparkContext(conf)
  val rawData = sc.textFile("data/covtype.data")
  val data = rawData.map{line=>
  	val values = line.split(",").map(_.toDouble)
  	//Array.init取出除最后一个
  	val featureVector = Vectors.dense(values.init)
  	val label = values.last -1   //决策树要求label从0开始，所以要减1
  	LabeledPoint(label,featureVector)
  }
  
   val Array(trainData,cvData,testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
   trainData.cache
   testData.cache
   
   def getMetrics(model : DecisionTreeModel, data : RDD[LabeledPoint]):MulticlassMetrics ={
     val predictionAndLabels = data.map{ example  =>
       (model.predict(example.features), example.label)
     }
     new MulticlassMetrics(predictionAndLabels)
   }
   def model = DecisionTree.trainClassifier(trainData,7, Map[Int,Int](),"gini",4,100)
   val metrics = getMetrics(model, cvData)
   metrics.confusionMatrix
   
   
  // data.map(_.label).distinct //查看label的取值范围  7个
   //准确度，
   metrics.precision
   //0.69999  70%是准确的
   (0 until 7).map(cat =>
   	 (cat, metrics.precision(cat),metrics.recall(cat))
   )
 // res13: scala.collection.immutable.IndexedSeq[(Int, Double, Double)] = 
   Vector((0,0.6742878737776938,0.677198975234842), 
       (1,0.7269495450246242,0.7823721436343852), 
       (2,0.6442368923795373,0.8487136465324385), 
       (3,0.5688073394495413,0.4542124542124542), 
       (4,0.0,0.0), 
       (5,0.7529411764705882,0.03825463239689181), 
       (6,0.7036450079239303,0.43106796116504853))
       
     
    def classProbabilities(data: RDD[LabeledPoint]):Array[Double] ={
     val countByCategory = data.map(_.label).countByValue
     val counts = countByCategory.toArray.sortBy(_._1).map(_._2)
     counts.map(_.toDouble/ counts.sum)
   }
   val trainPriorProbablities = classProbabilities(trainData)
 //  trainPriorProbablities: Array[Double] = Array(0.3647042106758869, 0.4874097389372338, 0.06165955486279457, 0.004682595385006222, 0.016344949040444708, 0.029977222133713396, 0.03522172896492037)
   val cvPriorProbalities = classProbabilities(cvData)
//   cvPriorProbalities: Array[Double] = Array(0.36275083468144426, 0.49029360134925826, 0.06154269782810725, 0.004698309985199463, 0.01646989983822669, 0.028792207345196708, 0.03545244897256738)

   //得到的结果是猜测的准确度
   trainPriorProbablities.zip(cvPriorProbalities).map{
     case(trainProb,cvProb)=>trainProb * cvProb
   }.sum
      //   res14: Double = 0.37746833532626245

       
    
  val evaluations =
    for(impurity<- Array("gini","entropy");
    		depth <- Array(1,20); 
    		bins <- Array(10,300)) 
      yield{
      val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), impurity, depth, bins)
      val predictionAndLabels = cvData.map(example=>
      (model.predict(example.features), example.label)
      )
      val accuracy = new MulticlassMetrics(predictionAndLabels).precision
      ((impurity,depth,bins), accuracy)
    }
      
   evaluations.sortBy(_._2).reverse.foreach(println)
   
   val data2 = rawData.map{line=>
   	val values = line.split(",").map(_.toDouble)
   	val wilderness = values.slice(10,14).indexOf(1.0).toDouble
   	val soil = values.slice(14,54).indexOf(1.0).toDouble
   	val featureVector = Vectors.dense(values.slice(0,10) :+ wilderness :+ soil)
   	val label = values.last -1
   	LabeledPoint(label, featureVector)
   }
      val Array(trainData2,cvData2,testData2) = data2.randomSplit(Array(0.8, 0.1, 0.1))
   trainData2.cache
   testData2.cache
    val evaluations2 =
    for(impurity<- Array("gini","entropy");
    		depth <- Array(10,20,30); 
    		bins <- Array(40,300)) 
      yield{
      val model = DecisionTree.trainClassifier(trainData2, 7, Map[Int,Int](10->4,11->40), impurity, depth, bins)
      val predictionAndLabels = cvData2.map(example=>
      (model.predict(example.features), example.label)
      )
      val accuracy = new MulticlassMetrics(predictionAndLabels).precision
      ((impurity,depth,bins), accuracy)
    }
  evaluations2.sortBy(_._2).reverse.foreach(println)
  
 val model2 =  RandomForest.trainClassifier(trainData2, 7, Map[Int,Int](10->4,11->40), 20, "auto", "entropy", 30, 300)
       val predictionAndLabels = cvData2.map(example=>
      (model2.predict(example.features), example.label)
      )
      val accuracy = new MulticlassMetrics(predictionAndLabels).precision
      (("entropy",30,300), accuracy)
  
   
  
  
}