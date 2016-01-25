package org.apache.spark.rdd

object RddTest extends App{
		  val slices = ParallelCollectionRDD.slice(1 to 100, 2).toArray
		println(slices)
}