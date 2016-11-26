import scala.io.Source

/**
  * Created by root on 26/11/16.
  */
object ConfigReader {
  def load(path: String): Map[String, String] = {
    val src = Source.fromFile(path)

    val propertyMap = src.getLines().filterNot(line => line.startsWith("#") || line.trim().equals(""))
      .map(line => {
        val pair = line.split("=")
        pair(0).trim() -> pair(1).trim()
      }).toMap
    propertyMap
  }
}
