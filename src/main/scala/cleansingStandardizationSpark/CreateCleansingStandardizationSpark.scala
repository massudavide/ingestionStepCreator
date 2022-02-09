package cleansingStandardizationSpark
import auxFunctions.extractValuesFromDDL.{getDecimalFromDDL, getNumericFromDDL}
import auxFunctions.regexAux.{getValuesInRoundBrackets, regexValueWithoutSome}
import cleansingStandardizationSpark.cleanStandSparkAux.{fromStringToDecimal, fromStringToTimestamp, getDate}

import io.circe.Json
import io.circe.syntax._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex


object CreateCleansingStandardizationSpark {

  def main(DDLToList: List[String], tableName: String, checkColumn: String, sourceSystemName: String, ingestionMode: String) = {
    val tabellaRaw = "${hive.db.raw}.r_" + tableName.toLowerCase()
    val tabella_curated = "${hive.db.curated}.c_" + tableName.toLowerCase() + "_tmp"
    val raw_location = "${environment.datalake.hdfs.uri}/raw_data/${hive.db.raw}/r_" + tableName.toLowerCase()
    val curated_lacation = "${environment.datalake.hdfs.uri}/curated_data/${hive.db.curated}/c_" + tableName.toLowerCase() + "_tmp"

    // lista per contenere LISTA_FUNZIONI
    var listaFunzioni = ListBuffer[Json]()

    // "NOME_FUNZIONE" : "fromStringToTimestamp"
    fromStringToTimestamp(DDLToList, listaFunzioni)

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
      "TABELLA_CURATED" -> tabella_curated.asJson,
      "FLAG_CLEANS_STAND"-> true.asJson,
      "RAW_LOCATION"-> raw_location.asJson,
      "CURATED_LOCATION"-> curated_lacation.asJson,
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

    val output_path = "src/main/output/src/main/resources/deploy/hdfs/layer_curated/cleansing_standardization_spark/conf/" + sourceSystemName + "/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get( output_path + "c_" + tableName.toLowerCase() +".json"), jsonObj.toString().getBytes(StandardCharsets.UTF_8))
  }
}
