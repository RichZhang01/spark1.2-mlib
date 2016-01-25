package advanced.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import com.cloudera.datascience.common.XmlInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import scala.xml._
import org.apache.spark.rdd.RDD
/***
 * 
 * spark-shell --master spark://dchdmaster2:7077 --jars common-1.0.2.jar,cloud9-1.5.0.jar,stanford-corenlp-3.4.1.jar,bliki-core-3.0.19.jar --executor-memory 10g --driver-memory 10g
 * 
 * 
 */
object Ch7_GraphX {
	
  def main(args: Array[String]) {
  	  val conf = new SparkConf().setMaster("local").setAppName("ch1Demo")
  	  val sc = new SparkContext(conf)
//  	  sc.setLogLevel("WARN")
  	  val path = "data/ch7/medline"
  	  val mediline_raw = loadMedline(sc, path)
  	  
  	  val cit = <MedlineCitation>data</MedlineCitation>
  	  val raw_xml = mediline_raw.take(1)(0)
  	  val elem = XML.loadString(raw_xml)
  	  elem.label
  	  elem.attributes
  	  elem \ "MeshHeadingList"   //只对节点的直接子节点有效
//  	  res5: scala.xml.NodeSeq = 
//NodeSeq(<MeshHeadingList>
//<MeshHeading>
//<DescriptorName UI="D001519" MajorTopicYN="N">Behavior</DescriptorName>
//</MeshHeading>
//<MeshHeading>
//<DescriptorName UI="D000013" MajorTopicYN="N">Congenital Abnormalities</DescriptorName>
//</MeshHeading>
//<MeshHeading>
//<DescriptorName UI="D006233" MajorTopicYN="N">Disabled Persons</DescriptorName>
//</MeshHeading>
//  	  scala>   elem \ "MeshHeading"
//res6: scala.xml.NodeSeq = NodeSeq()
  	  elem \\ "MeshHeading" //对间接子节点有效
  	  (elem \\ "DescriptorName").map(_.text)
  	  majorTopics(elem)
//  	  res10: Seq[String] = List(Intellectual Disability, Maternal-Fetal Exchange, Pregnancy Complications)
 
  
  	  val mxml : RDD[Elem] = mediline_raw.map(XML.loadString)
  	  val medline= mxml.map(majorTopics).cache
  	  medline.take(1)(0)
//  	  scala>     medline.take(1)
//res12: Array[Seq[String]] = Array(List(Intellectual Disability, Maternal-Fetal Exchange, Pregnancy Complications))
//
//scala> res12(0)
//res13: Seq[String] = List(Intellectual Disability, Maternal-Fetal Exchange, Pregnancy Complications)
  	  	
  	  medline.count
  	  val topics : RDD[String] = medline.flatMap(mesh=>mesh)
  	  val topicCounts =  topics.countByValue
//  	  scala>     val topicCounts =  topics.countByValue
//topicCounts: scala.collection.Map[String,Long] = Map(Dental Implantation, Subperiosteal -> 3, Odontogenic Tumors -> 10, Spinal Cord Diseases -> 25, Family Leave -> 2, Crystallography, X-Ray -> 2, Ethylene Glycol -> 2, Cisplatin -> 1, Type A Personality -> 3, G(M1) Ganglioside -> 4, 2-Naphthylamine -> 1, Pancreatic Diseases -> 42, Lung Compliance -> 6, Streptolysins -> 2, Vicia faba -> 1, Direct Service Costs -> 1, Lichen Planus -> 11, Case-Control Studies -> 11, Superoxide Dismutase -> 3, Verbal Behavior -> 37, Phantom Limb -> 4, Leeches -> 18, Leisure Activities -> 25, Collective Bargaining -> 19, Limulus Test -> 1, Dentition, Mixed -> 1, Emergency Medical Service Communication Systems -> 4, Psychopharmacology -> 32, Crying -> 4, Leishmania tropica -> 2, Microscopy, Confocal -> 6, Benz...
//scala> topics.first
  	  topicCounts.size
//  	  scala> topicCounts.size
//res25: Int = 14548
  	  
  	  val tcSeq = topicCounts.toSeq
  	  tcSeq.sortBy(-_._2).take(10).foreach(println)
//scala>   tcSeq.sortBy(-_._2).take(10).foreach(println)
//(Research,1649)
//(Disease,1349)
//(Neoplasms,1123)
//(Tuberculosis,1066)
//(Public Policy,816)
//(Jurisprudence,796)
//(Demography,763)
//(Population Dynamics,753)
//(Economics,690)
//(Medicine,682)
  	  
  	  val valueDist = topicCounts.groupBy(_._2).mapValues(_.size)
  	 valueDist.toSeq.sorted.take(10).foreach(println)
  	 
  	    val topicPairs = medline.flatMap(t=> t.sorted.combinations(2))
//  	    combinations //是Iterator[Seq[]]的方法
//  	    val cooccurs = 
  	  
  }
  
  def loadMedline(sc:SparkContext, path:String) = {
    @transient val conf = new Configuration
    conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val in = sc.newAPIHadoopFile(path,classOf[XmlInputFormat], classOf[LongWritable],classOf[Text],conf)
    in.map(_._2.toString)
//    res8: scala.collection.immutable.Seq[String] = List(Behavior, Congenital Abnormalities, Disabled Persons, Disease, Intellectual Disability, Intelligence, Maternal-Fetal Exchange, Personality, Pregnancy, Pregnancy Complications, Psychology, Reproduction, Research)
  }
  
  def majorTopics(elem:Elem):Seq[String] = {
    val dn = elem \\"DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text=="Y")
    mt.map(n => n.text)
  }
  
  
  

  
  
  
  
  
  
  
  
  
}