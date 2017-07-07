
import java.util.Properties
import scala.collection.JavaConverters._


object PropertiesLoader {
  
  def load(filename:String) : Properties = {
    val inputStream = PropertiesLoader.getClass.getClassLoader.getResourceAsStream(filename)
    val props = new Properties()
    props.load(inputStream)
    inputStream.close()
    props
  }
  def loadAsMap(filename:String): Map[String, String] = {
    load(filename).asScala.toMap[String, String]
  }
  
  def main(args:Array[String])={
    print(loadAsMap("kafka.properties"))
  }
}