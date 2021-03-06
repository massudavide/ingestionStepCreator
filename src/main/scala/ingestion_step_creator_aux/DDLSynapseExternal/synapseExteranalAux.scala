package ingestion_step_creator_aux.DDLSynapseExternal

import ingestion_step_creator_aux.DDLHiveCurated.hiveCuratedAux
import ingestion_step_creator_aux.auxFunctions.extractValuesFromDDL.{getDatesFromDDL, getDecimalFromDDL, getNumericFromDDL}
import ingestion_step_creator_aux.auxFunctions.regexAux.{getContentInRoundBracket, getValuesInRoundBrackets, regexValueWithoutSome}
import ingestion_step_creator_aux.cleansingStandardizationSpark.cleanStandSparkAux

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

object synapseExteranalAux {


  def replaceDecOrNumericRaw(splittedLine: Array[String], line: String) = {
    var appendElemString = ""
    val findVarcharLength = new Regex("""\((.*?)\)""")

    val varcharLengthRegex = regexValueWithoutSome(findVarcharLength, splittedLine(1))
    val varcharLength = getValuesInRoundBrackets(varcharLengthRegex)._1

    //    val lineSplitted = line.strip().split(" ")
    splittedLine(0) = splittedLine(0).toLowerCase()
    splittedLine(1) = "varchar(" + varcharLength + ") COLLATE Latin1_General_CI_AS"
    appendElemString += "\t" + splittedLine.mkString(" ") + "\n"

    appendElemString
  }

  def replaceDecOrNumericRaw(numOrDecMap: mutable.Map[String, ArrayBuffer[String]], splittedLine: Array[String], line: String) = {
    var appendElemString = ""
    val findVarcharLength = new Regex("""\((.*?)\)""")
    for ((k, v) <- numOrDecMap) {
      breakable {
        val varcharLengthRegex = regexValueWithoutSome(findVarcharLength, k)
        val varcharLength = getValuesInRoundBrackets(varcharLengthRegex)._1
        if (v contains splittedLine(0)) {
          val lineSplitted = line.strip().split(" ")
          lineSplitted(0) = lineSplitted(0).toLowerCase()
          lineSplitted(1) = "varchar(" + varcharLength + ")"
          appendElemString += "\t" + lineSplitted.mkString(" ") + "\n"
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
            var splittedLine = lineRaw.strip().split(" ")
            if (splittedLine(0) == currentValue && !splittedLine.contains("CONSTRAINT")) {
              var replacedLine = lineRaw
                .replace("\t", "")
                .replace(splittedLine(0), splittedLine(0).toLowerCase())

              // remove IDENTITY from raw
              if(replacedLine.contains("IDENTITY")){
                val identity = removeIdentity(replacedLine, splittedLine)
                replacedLine = identity._1
                splittedLine = identity._2
              }
              // remove DEAFAULT from raw
              if(replacedLine.contains("DEFAULT")){
                val default = removeDefault(replacedLine, splittedLine)
                replacedLine = default._1
                splittedLine = default._2
              }

              appendElemString += "\t" + replacedLine + "\n"
              break
            }
          }
        }
      }
    }
    appendElemString
  }

  def appendDates(dateMap: ArrayBuffer[String], DDLToList: List[String], dateType: String) = {
    var appendElemString = ""
    for (data <- dateMap) {
      for (line <- DDLToList) {
        breakable{
          var splittedLine = line.strip().split(" ")
          if (splittedLine(0) == data && !line.contains("CONSTRAINT")) {
            var replacedLine = line
              .replace("\t", "")
              .replace(splittedLine(0), splittedLine(0).toLowerCase())
              .replace(splittedLine(1), dateType)

            // remove IDENTITY from raw
            if(replacedLine.contains("IDENTITY")){
              val identity = removeIdentity(replacedLine, splittedLine)
              replacedLine = identity._1
              splittedLine = identity._2
            }
            // remove DEAFAULT from raw
            if(replacedLine.contains("DEFAULT")){
              val default = removeDefault(replacedLine, splittedLine)
              replacedLine = default._1
              splittedLine = default._2
            }

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
    val newSplittedLine = newLine.strip().split(" ")
    return  (newLine, newSplittedLine)
  }

  def removeDefault(line: String, splittedLine: Array[String]): (String, Array[String]) ={
    println("\n\n" + line)
    var indice = 0
    for(i <- splittedLine.indices){
      if(splittedLine(i) == "DEFAULT"){
        indice = i
      }
    }
    val default = " " + splittedLine(indice) + " " + splittedLine(indice + 1)
    val newLine = line.replace(default, "")
    val newSplittedLine = newLine.strip().split(" ")
    println(newLine + "\n" + newSplittedLine.mkString(" ") + "\n\n")
    return  (newLine, newSplittedLine)
  }


  def allignment_Ext_table(DDLToList: List[String]): String = {
    // get Date from DDL
    //    val getDateTimeToArray = getDatesFromDDL(DDLToList,"datetime")
    //     TODO aggiungere date
    //    val getDateToArray = getDatesFromDDL(DDLToList,"date")
    //     get Numeric from DDL
    //    val getNumericToMap = getNumericFromDDL(DDLToList)
    //     get decimal from DDL
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
            println("-------> " + line + "\n" + splittedLine.mkString(" "))
          }

          // datetime
          //          if (getDateTimeToArray contains splittedLine(0)) {
          //            val replacedLine = line
          //              .replace(splittedLine(0), splittedLine(0).toLowerCase())
          //              .replace(splittedLine(1), "varchar(30) COLLATE Latin1_General_CI_AS")
          //            allignmentTableString += "\t" + replacedLine + "\n"
          //            break
          //          }
          if (splittedLine(1).startsWith("datetime")) {
            splittedLine(0) = splittedLine(0).toLowerCase()
            splittedLine(1) = "varchar(30) COLLATE Latin1_General_CI_AS"
            allignmentTableString += "\t" + splittedLine.mkString(" ") + "\n"
            break
          }

          // date
          if (splittedLine(1) == "date") {
            splittedLine(0) = splittedLine(0).toLowerCase()
            splittedLine(1) = "varchar(30) COLLATE Latin1_General_CI_AS"
            allignmentTableString += "\t" + splittedLine.mkString(" ") + "\n"
            break
          }

          // numeric
          if(splittedLine(1).startsWith("decimal") || splittedLine(1).startsWith("numeric")){
            val numericRawsToString = replaceDecOrNumericRaw(splittedLine, line)
            allignmentTableString += numericRawsToString
            break()
          }


          //          // decimal
          //          val decimalRawsToString = replaceDecOrNumericRaw(getDecimalToMap, splittedLine, line)
          //          if (decimalRawsToString != "") {
          //            allignmentTableString += decimalRawsToString
          //            break()
          //          }

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
    allignmentTableString += "\t" + "d_caricamento varchar(30) COLLATE Latin1_General_CI_AS NULL"

    //    // datetime
    //    allignmentTableString += appendDates(getDateTimeToArray, DDLToList, "datetime")
    //    // date
    //    allignmentTableString += appendDates(getDateToArray, DDLToList, "date")

    // TODO rimuovere riga
    //    allignmentTableString += "\t" + "d_caricamento datetime NULL,\n"

    // numeric
    //    allignmentTableString += appendDecOrNum(getNumericToMap, DDLToList)
    // decimal
    //    allignmentTableString += appendDecOrNum(getDecimalToMap, DDLToList)

    // remove ',' from last raw
    //    allignmentTableString = allignmentTableString.substring(0, allignmentTableString.length - 2)

    return allignmentTableString
  }
}
