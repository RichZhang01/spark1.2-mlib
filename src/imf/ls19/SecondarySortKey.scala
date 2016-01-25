package imf.ls19

class SecondarySortKey(val first:Int,val second:Int) extends Ordered[SecondarySortKey] with Serializable{
	 
 def  compare(other: SecondarySortKey):Int = {
   if(this.first - other.first!=0) {
     first -other.first
   }else{
     other.second - this.second
   }
 }
  
}
object SecondarySortKey{
  def apply(first:Int,second:Int) :SecondarySortKey = new SecondarySortKey(first,second)
}