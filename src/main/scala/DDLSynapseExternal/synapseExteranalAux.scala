package DDLSynapseExternal

import DDLHiveCurated.hiveCuratedAux
import auxFunctions.extractValuesFromDDL.{getDatesFromDDL, getDecimalFromDDL, getNumericFromDDL}
import auxFunctions.regexAux.{getContentInRoundBracket, getValuesInRoundBrackets, regexValueWithoutSome}
import cleansingStandardizationSpark.cleanStandSparkAux

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

object synapseExteranalAux {

  def replaceDecOrNumericRaw(numOrDecMap: mutable.Map[String, ArrayBuffer[String]], splittedLine: Array[String], line: String) = {
    var appendElemString = ""
    val findVarcharLength = new Regex("""\((.*?)\)""")
    for ((k, v) <- numOrDecMap) {
      breakable {
        val varcharLengthRegex = regexValueWithoutSome(findVarcharLength, k)
        val varcharLength = getValuesInRoundBrackets(varcharLengthRegex)._1 + 4
        if (v contains splittedLine(0)) {
          val replacedLine = line
            .replace("\t", "")
            .replace(splittedLine(0), splittedLine(0).toLowerCase() + "_raw")
            .replace(splittedLine(1), "varchar(" + varcharLength + ")")
          appendElemString += "\t" + replacedLine + "\n"
          break
        }
      }
    }
    appendElemString
  }

  def appendDecOrNum(numOrDecMap: mutable.Map[String, ArrayBuffer[String]], DDLToList:  List[String]) = {
    var appendElemString = ""
    for ((k,v) <- numOrDecMap) {
      for(currentValue <- v){
        breakable{
          for (lineRaw <- DDLToList) {
            val splittedLine = lineRaw.strip().split(" ")
            if (splittedLine(0) == currentValue && !splittedLine.contains("CONSTRAINT")) {
              val replacedLine = lineRaw
                .replace("\t", "")
                .replace(splittedLine(0), splittedLine(0).toLowerCase())
              appendElemString += "\t" + replacedLine + "\n"
              break
            }
          }
        }
      }
    }
    appendElemString
  }

  def appendDates(dateMap: ArrayBuffer[String], DDLToList: List[String]) = {
    var appendElemString = ""
    for (data <- dateMap) {
      for (line <- DDLToList) {
        breakable{
          val splittedLine = line.strip().split(" ")
          if (splittedLine(0) == data && !line.contains("CONSTRAINT")) {
            val replacedLine = line
              .replace("\t", "")
              .replace(splittedLine(0), splittedLine(0).toLowerCase())
              .replace(splittedLine(1), "datetime")
            appendElemString += "\t" + replacedLine + "\n"
          }
          break()
        }
      }
    }
    appendElemString
  }

  def removeIdentity(line: String, splittedLine: Array[String]): (String, Array[String]) ={
    var indice = 0
    for(i <- splittedLine.indices){
      if(splittedLine(i).startsWith("IDENTITY")){
        indice = i
      }
    }
    val identity = " IDENTITY" + getContentInRoundBracket(splittedLine(indice))
    val newLine = line.replace(identity, "")
    val newSplittedLine = line.strip().split(" ")
    return  (newLine, newSplittedLine)
  }

  def removeDefault(line: String, splittedLine: Array[String]): (String, Array[String]) ={
    var indice = 0
    for(i <- splittedLine.indices){
      if(splittedLine(i) == "DEFAULT"){
        indice = i
      }
    }
    val default = " " + splittedLine(indice) + " " + splittedLine(indice + 1)
    val newLine = line.replace(default, "")
    val newSplittedLine = line.strip().split(" ")
    return  (newLine, newSplittedLine)
  }


  def allignment_Ext_table(DDLToList: List[String]): String = {
    // get Date from DDL
    val getDateToArray = getDatesFromDDL(DDLToList)
    // get Numeric from DDL
    val getNumericToMap = getNumericFromDDL(DDLToList)
    // get decimal from DDL
    val getDecimalToMap = getDecimalFromDDL(DDLToList)

    var allignmentTableString = ""
    for (lineRaw <- DDLToList) {
      var line = lineRaw.replace("\t", "")
      breakable{
        if (!line.contains("CONSTRAINT")) {

          var splittedLine = line.strip().split(" ")

          // remove IDENTITY from raw
          if(line.contains("IDENTITY")){
            println("\n\n" + line)
            val identity = removeIdentity(line, splittedLine)
            line = identity._1
            splittedLine = identity._2
          }

          // remove DEAFAULT from raw
          if(line.contains("DEFAULT")){
            val default = removeDefault(line, splittedLine)
            line = default._1
            splittedLine = default._2
          }

          // datetime
          if (getDateToArray contains splittedLine(0)) {
            val replacedLine = line
              .replace(splittedLine(0), splittedLine(0).toLowerCase() + "_raw")
              .replace(splittedLine(1), "varchar(30) COLLATE Latin1_General_CI_AS")
            allignmentTableString += "\t" + replacedLine + "\n"
            break
          }

          // numeric
          val numericRawsToString = replaceDecOrNumericRaw(getNumericToMap, splittedLine, line)
          if (numericRawsToString != "") {
            allignmentTableString += numericRawsToString
            break()
          }

          // decimal
          val decimalRawsToString = replaceDecOrNumericRaw(getDecimalToMap, splittedLine, line)
          if (decimalRawsToString != "") {
            allignmentTableString += decimalRawsToString
            break()
          }

          // varchar(MAX) to varchar(8000)
          if(line.contains("varchar(MAX)")){
            line = line.replace("varchar(MAX)", "varchar(8000)")
            splittedLine = line.strip().split(" ")
          }

          // varbinary(MAX) to varbinary(8000)
          if(line.contains("varbinary(MAX)")){
            line = line.replace("varbinary(MAX)", "varbinary(8000)")
            splittedLine = line.strip().split(" ")
          }

          // if I'm here I need to append the whole line
          val replacedLine = line
            .replace(splittedLine(0), splittedLine(0).toLowerCase())
          allignmentTableString += "\t" + replacedLine + "\n"

        }
      }
    }
    allignmentTableString += "\t" + "d_caricamento_raw varchar(30) COLLATE Latin1_General_CI_AS NULL,\n"

    // dates
    allignmentTableString += appendDates(getDateToArray, DDLToList)

    allignmentTableString += "\t" + "d_caricamento datetime NULL,\n"

    // numeric
    allignmentTableString += appendDecOrNum(getNumericToMap, DDLToList)
    // decimal
    allignmentTableString += appendDecOrNum(getDecimalToMap, DDLToList)

    // remove ',' from last raw
    allignmentTableString = allignmentTableString.substring(0, allignmentTableString.length - 2)

    return allignmentTableString
  }
}
