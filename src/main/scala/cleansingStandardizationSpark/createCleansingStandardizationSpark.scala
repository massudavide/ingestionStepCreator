package cleansingStandardizationSpark
import auxFunctions.extractValuesFromDDL.{getDecimalFromDDL, getNumericFromDDL}
import auxFunctions.regexAux.{getValuesInRoundBrackets, regexValueWithoutSome}
import cleansingStandardizationSpark.cleanStandSparkAux.{fromStringToDate, fromStringToDecimal, fromStringToTimestamp, getDate}
import io.circe.Json
import io.circe.syntax._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex


object createCleansingStandardizationSpark {

  def main(DDLToList: List[String], tableName: String, checkColumn: String, sourceSystemName: String,
           processName: String, ingestionMode: String, cleansingOutputPath: String) = {
    val tabellaRaw = "${hive.db.raw}.r_" + tableName.toLowerCase()

    val raw_location = "${environment.datalake.hdfs.uri}/raw_data/${hive.db.raw}/r_" + tableName.toLowerCase()


    // lista per contenere LISTA_FUNZIONI
    var listaFunzioni = ListBuffer[Json]()

    // "NOME_FUNZIONE" : "fromStringToTimestamp"
    fromStringToTimestamp(DDLToList, listaFunzioni)

    // "NOME_FUNZIONE" : "fromStringToDate"
    fromStringToDate(DDLToList, listaFunzioni)

    // "NOME_FUNZIONE" : "fromStringToDecimal"
    // numeric
    fromStringToDecimal(getNumericFromDDL(DDLToList), listaFunzioni)
    // decimal
    fromStringToDecimal(getDecimalFromDDL(DDLToList), listaFunzioni)

    //TODO implementare fromStringToDate

    val jsonObj = Json.obj(
      "DATA_CREAZIONE" -> getDate.asJson,
      "VERSIONE" -> "1.0".asJson,
      "TABELLA_RAW"-> tabellaRaw.asJson,

      if(processName=="summerbi")
        "TABELLA_CURATED" -> ("${hive.db.integrated}." + tableName.toLowerCase()).asJson
      else // processName=="dco"
        "TABELLA_CURATED" -> ("${hive.db.curated}.c_" + tableName.toLowerCase() + "_tmp").asJson,

      "FLAG_CLEANS_STAND"-> true.asJson,
      "RAW_LOCATION"-> raw_location.asJson,

      if(processName=="summerbi")
        "CURATED_LOCATION"->
          ("${environment.datalake.hdfs.uri}/integrated_data/${hive.db.integrated}/" + tableName.toLowerCase()).asJson
      else
        "CURATED_LOCATION"->
          ("${environment.datalake.hdfs.uri}/curated_data/${hive.db.curated}/c_" + tableName.toLowerCase() + "_tmp").asJson,


      "UPDATE_RAW_LOCATION"-> false.asJson,
      "PARTIONED_RAW_TABLE"-> false.asJson,
      "CHECK_COLUMN"-> checkColumn.asJson,
      "CHECK_COLUMN_TYPE"-> "timestamp".asJson,
      if(ingestionMode == "DELTA_DATE")
        "CHECK_COLUMN_OPERATOR" -> ">=".asJson
      else
        "CHECK_COLUMN_OPERATOR" -> ">".asJson,
      if(ingestionMode == "FULL") "WRITE_MODE" -> "overwrite".asJson
      else "WRITE_MODE" -> "append".asJson,
      "LISTA_FUNZIONI" -> listaFunzioni.asJson
    )

    val output_path = cleansingOutputPath + processName + "/" + sourceSystemName + "/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get( output_path + "c_" + tableName.toLowerCase() +".json"), jsonObj.toString().getBytes(StandardCharsets.UTF_8))
  }
}
