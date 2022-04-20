package ingestion_step_creator_aux.sqoopConfigFile

import ingestion_step_creator_aux.auxFunctions.extractValuesFromDDL.{getDatesFromDDL, getDecimalFromDDL, getNumericFromDDL}

import scala.util.matching.Regex

object sqoopAux {

  def populatesFieldMapColumnsJava(DDLToList: List[String]): String = {
    var mapColumnsString = ""
    val dateTime = getDatesFromDDL(DDLToList, "datetime")
    // TODO aggiungere date
    dateTime.foreach(mapColumnsString += _ + "=String,")

    val dates = getDatesFromDDL(DDLToList, "date")
    // TODO aggiungere date
    dates.foreach(mapColumnsString += _ + "=String,")

    // TODO rimuove questa parte
//    val numeric = getNumericFromDDL(DDLToList)
//    for ((k,v) <- numeric){
//      v.foreach(mapColumnsString += _ + "=String,")
//    }
//
//    val decimal = getDecimalFromDDL(DDLToList)
//    for ((k,v) <- decimal){
//      v.foreach(mapColumnsString += _ + "=String,")
//    }

    if(mapColumnsString != "")
      mapColumnsString = mapColumnsString.substring(0, mapColumnsString.length - 1)
    mapColumnsString
  }

  def getFirstPrimaryKey(DDLToList: List[String]): String = {
    for (line <- DDLToList) {
      if (line.contains("CONSTRAINT") && line.contains("PRIMARY KEY")) {
        val primaryKeyPattern = new Regex("""(?<=\()(\w+)""")
        // get first primary key
        val PrimaryKeyRegex = primaryKeyPattern findFirstMatchIn line
        return PrimaryKeyRegex.mkString
      }
    }
    return "ATTENZIONE: NESSUNA RIGA CONTENENTE CONSTRAINT"
  }
}
