package ingestion_step_creator_aux.cleansingStandardizationSpark

import ingestion_step_creator_aux.DDLHiveCurated.hiveCuratedAux
import ingestion_step_creator_aux.auxFunctions.extractValuesFromDDL.getDatesFromDDL
import ingestion_step_creator_aux.auxFunctions.regexAux.{getValuesInRoundBrackets, regexValueWithoutSome}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex
import io.circe.Json
import io.circe.syntax._

import scala.language.postfixOps


object cleanStandSparkAux {

  def DatesPlusDCaricamento(DDLToList: List[String]): ArrayBuffer[String] = {
    var dateTimeArray = getDatesFromDDL(DDLToList, "datetime")
    // TODO aggiungere date
    dateTimeArray += "d_caricamento"
  }
  def fromStringToTimestamp(DDLToList: List[String], stringToTimestampList: ListBuffer[Json]) = {
    val datesFunJson = Json.obj(
      "NOME_FUNZIONE" -> "fromStringToTimestamp".asJson,
      "LISTA_PARAMETRI" -> Json.arr("yyyy-MM-dd HH:mm:ss.SSS".asJson),
      "LISTA_CAMPI" -> cleanStandSparkAux.DatesPlusDCaricamento(DDLToList).asJson
    )
    stringToTimestampList += datesFunJson
  }

  def fromStringToDate(DDLToList: List[String], stringToTimestampList: ListBuffer[Json]) = {
    val getDates = getDatesFromDDL(DDLToList, "date")
    if(getDates nonEmpty){
      val datesFunJson = Json.obj(
        "NOME_FUNZIONE" -> "fromStringToDate".asJson,
        "LISTA_PARAMETRI" -> Json.arr("yyyy-MM-dd".asJson),
        "LISTA_CAMPI" -> getDates.asJson
      )
      stringToTimestampList += datesFunJson
    }
  }

  def fromStringToDecimal(getNumericOrDecimalMap:  mutable.Map[String, ArrayBuffer[String]], stringToDecimalList: ListBuffer[Json]) = {
    val valuesInRoundBracketsRegex = new Regex("""\((.*?)\)""")
    for ((k,v) <- getNumericOrDecimalMap){
      val valuesInRoundBrackets = regexValueWithoutSome(valuesInRoundBracketsRegex, k)

      val numeri = getValuesInRoundBrackets(valuesInRoundBrackets)
      val primo_numero = numeri._1
      val secondo_numero = numeri._2

      val NumericFunJson = Json.obj(
        "NOME_FUNZIONE" -> "fromStringToDecimal".asJson,
        "LISTA_PARAMETRI" -> Json.arr(secondo_numero.asJson, primo_numero.asJson),
        "LISTA_CAMPI" -> v.asJson
      )
      stringToDecimalList += NumericFunJson
    }
  }

  def getDate() = {
    import java.util.Calendar
    import java.text.SimpleDateFormat

    val dateTime = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
    val data = dateFormat.format(dateTime)
    data
  }

}
