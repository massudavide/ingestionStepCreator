package DDLHiveCurated

import DDLhiveRaw.hiveRawAux
import auxFunctions.extractValuesFromDDL.{getDatesFromDDL, getDecimalFromDDL, getNumericFromDDL}
import auxFunctions.regexAux.{getValuesInRoundBrackets, regexValueWithoutSome}
import cleansingStandardizationSpark.cleanStandSparkAux

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import scala.util.matching.Regex

object hiveCuratedAux {

  def appendDecimalOrNumeric(numOrDecMap: mutable.Map[String, ArrayBuffer[String]]): String = {
    var appendElemString = ""
    val valuesInBracketRegex = new Regex("""\((.*?)\)""")
    // numeric
    for ((k,v) <- numOrDecMap) {

      val numeri = getValuesInRoundBrackets(regexValueWithoutSome(valuesInBracketRegex, k))
      for(currentValue <- v){
        appendElemString += "\t" + currentValue.toLowerCase() + " DECIMAL" + numeri + ",\n"
      }
    }
    appendElemString
  }

  def curated_hive_table(DDLToList: List[String]): String = {

    // get DDL list from Hive Raw Table
    val hiveRawType = hiveRawAux.mapIntoHiveType(DDLToList)
    if (hiveRawType == "") return "Error: hive Raw table empty"
    val hiveRawToList = hiveRawType.strip().split("\n")

    val getDateToArray = getDatesFromDDL(DDLToList).map(_.toLowerCase())
    val getNumericToMap = getNumericFromDDL(DDLToList)
    val getDecimalToMap = getDecimalFromDDL(DDLToList)

    // TODO accenti! nel Raw Hive ci possono essere stringe con backtick
    // quindi nel confronto con datetime/numeric/decimal sbaglia
    // es stringa 'unità_qualcosa' != unità_qualcosa
    var curatedTable = ""
    // per ogni riga di Hive Raw Table
    for (rawHive <- hiveRawToList) {
      breakable{
        val rawHiveSplitted = rawHive.strip().split(" ")

        if (getDateToArray contains rawHiveSplitted(0)) {
          curatedTable += rawHive.replace(rawHiveSplitted(0), rawHiveSplitted(0) + "_raw") + "\n"
          break
        }

        for ((k, v) <- getNumericToMap) {
          val values = v.map(_.toLowerCase())
          if(values contains rawHiveSplitted(0)) {
            curatedTable += rawHive.replace(rawHiveSplitted(0), rawHiveSplitted(0) + "_raw") + "\n"
            break
          }
        }

        for ((k, v) <- getDecimalToMap) {
          val values = v.map(_.toLowerCase())
          if(values contains rawHiveSplitted(0)) {
            curatedTable += rawHive.replace(rawHiveSplitted(0), rawHiveSplitted(0) + "_raw") + "\n"
            break
          }
        }

        // anche nel caso di d_caricamento dobbiamo aggiungere suffix _raw
        if (rawHiveSplitted(0) == "d_caricamento") {
          curatedTable += rawHive.replace(rawHiveSplitted(0), rawHiveSplitted(0) + "_raw") + ",\n"
          break
        }


        if(rawHive.startsWith("\t"))
          curatedTable += rawHive + "\n"
        else curatedTable += "\t" + rawHive + "\n"
      }
    }

    // ora che abbiamo aggiunto il suffisso _raw
    // bisogna aggiungere tutte le date, numeric e decimal

    // datetime
    for (data <- getDateToArray) curatedTable += "\t" + data.toLowerCase() + " TIMESTAMP,\n"

    curatedTable += "\t" + "d_caricamento TIMESTAMP,\n"

    // numeric
    curatedTable += appendDecimalOrNumeric(getNumericToMap)

    // decimal
    curatedTable += appendDecimalOrNumeric(getDecimalToMap)

    // remove ',\n' from last raw
    curatedTable = curatedTable.substring(0, curatedTable.length - 2)

    curatedTable
  }

}
