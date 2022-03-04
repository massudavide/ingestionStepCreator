package auxFunctions

import scala.collection.mutable.ArrayBuffer

object extractValuesFromDDL {
  def getNumericFromDDL(DDLToList: List[String]): scala.collection.mutable.Map[String, ArrayBuffer[String]] = {
    var mapWithNumeric = scala.collection.mutable.Map[String, ArrayBuffer[String]]()
    for (lineRaw <- DDLToList) {
      val line = lineRaw.strip().split(" ")
      if (line(1).contains("numeric")) {

        val key = line(1)
        val value = line(0)

        val valueArray = new ArrayBuffer[String]()
        if (mapWithNumeric.exists(x => x._1 == key)){
          val valueInMap = mapWithNumeric.get(key) match {
            case Some(x) => x
          }
          valueArray ++= valueInMap
          valueArray += value
        }
        else{
          valueArray += value
        }
        mapWithNumeric(key) = valueArray
      }
    }
    return  mapWithNumeric
  }

  def getDecimalFromDDL(DDLToList: List[String]): scala.collection.mutable.Map[String, ArrayBuffer[String]] = {
    var mapWithDecimal = scala.collection.mutable.Map[String, ArrayBuffer[String]]()
    for (lineRaw <- DDLToList) {
      val line = lineRaw.strip().split(" ")
      if (line(1).contains("decimal")) {
        val key = line(1)
        val value = line(0)

        val valueArray = new ArrayBuffer[String]()
        if (mapWithDecimal.exists(x => x._1 == key)){
          val valueInMap = mapWithDecimal.get(key) match {
            case Some(x) => x
          }
          valueArray ++= valueInMap
          valueArray += value
        }
        else{
          valueArray += value
        }
        mapWithDecimal(key) = valueArray
      }
    }
    return  mapWithDecimal
  }

  def getDatesFromDDL(DDLToList: List[String], dateType: String): ArrayBuffer[String] = {
    var datesFromDDLArray = scala.collection.mutable.ArrayBuffer.empty[String]
    for (lineRaw <- DDLToList) {
      val line = lineRaw.strip().split(" ")
      if (line(1) == dateType) {
        datesFromDDLArray += line(0)
      }
    }
    return datesFromDDLArray
  }
}
