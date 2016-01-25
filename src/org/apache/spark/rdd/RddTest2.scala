package org.apache.spark.rdd

import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.Partition

object RddTest2 {
//  override def getPartitions: Array[Partition] = {
//    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
//    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
//  }
  
//  def getPartitions :Array[Partition] = {
//    val slices = ParallelCollectionRDD.slice(1 to 3, 2).toArray
//    slices.indices.map(i=> new  ParallelCollectionPartition(id,i,slices(i))).toArray
//  }
	
  def main(args: Array[String]) {
   val slices = ParallelCollectionRDD.slice(1 to 100, 3).toArray
		   println(slices)
		slices.foreach(println)
	val r =	slices.indices
		println(r)

  
  }
}