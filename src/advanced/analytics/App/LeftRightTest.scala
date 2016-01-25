package advanced.analytics.App

object LeftRightTest {
	
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
  
  def testLeftRight(s:String)= {
    s match{
      case "hello" => Left("hello")
      case _ => Right(3)
    }
  }
  def main(args: Array[String]) {
	  println(testLeftRight("hello"))
	  println("---------")
	  println(testLeftRight("kskd"))
	  
  }
}