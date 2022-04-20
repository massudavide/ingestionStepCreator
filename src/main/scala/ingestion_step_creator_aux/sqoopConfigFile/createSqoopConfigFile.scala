package ingestion_step_creator_aux.sqoopConfigFile
import ingestion_step_creator_aux.auxFunctions.regexAux

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import ingestion_step_creator_aux.sqoopConfigFile.sqoopAux.{getFirstPrimaryKey, populatesFieldMapColumnsJava}

object createSqoopConfigFile {

  def explicit_select(DDLToList: List[String]): String ={
    var select_string = "querySource=\"SELECT\n"
    for(i <- DDLToList){
      val splitted_line = i.strip().split(" ")
      if(splitted_line(1).startsWith("numeric") || splitted_line(1).startsWith("decimal")){
        val get_content_in_round_bracket = regexAux.getContentInRoundBracket(splitted_line(1))
        select_string += "                cast(" + splitted_line(0) + " as varchar(" + (regexAux.getValuesInRoundBrackets(get_content_in_round_bracket)._1 + 2) + ")) as " + splitted_line(0) + ",\n"
      }
      else if(splitted_line(0).startsWith("CONSTRAINT")){      }
      else{
        select_string += "                " + splitted_line(0) + ",\n"
      }
    }
    select_string += "            '$(date '+%Y-%m-%d %H:%M:%S.%3N')' AS d_caricamento FROM \" + databaseName + \".\" + tableName +\" t WHERE 1=1 and \\\\$CONDITIONS\\\"\\n"

    select_string
  }


  def contains_numeric_or_decimal(DDLToList: List[String]): Boolean ={
    for(i <- DDLToList){
      val splitted_line = i.strip().split(" ")
      if(splitted_line(1).startsWith("numeric") || splitted_line(1).startsWith("decimal")){
        return true
      }
    }
    return false
  }

  def main(DDLToList: List[String], databaseName: String, sourceSystemName: String, tableName: String,
           checkColumn: String, ingestionMode: String, sqoopOutputPath: String) = {
    var sqoopConfString = ""

    sqoopConfString += "connect=\"${environment." + sourceSystemName + ".jdbc.url}\"\n"
    sqoopConfString += "username=\"${environment." + sourceSystemName + ".jdbc.username}\"\n"
    sqoopConfString += "hdfsPasswordPath=\"/user/${environment.user}/database/" + sourceSystemName + ".password.jceks\"\n"
    sqoopConfString += "aliasPass=\"" + sourceSystemName + ".alias\"\n"
    sqoopConfString += "tableTarget=${hive.db.raw}.r_" + tableName.toLowerCase() + "\n"
    sqoopConfString += "dir=\"${environment.datalake.hdfs.uri}/raw_data/${hive.db.raw}/r_" + tableName.toLowerCase() + "\"\n"
    sqoopConfString += "splitValue=" + getFirstPrimaryKey(DDLToList) + "\n"
    sqoopConfString += "checkColumn=" + checkColumn + "\n"
    if(contains_numeric_or_decimal(DDLToList)){
      sqoopConfString += explicit_select(DDLToList)
    }
    else{
      sqoopConfString += "querySource=\"SELECT t.*, '$(date '+%Y-%m-%d %H:%M:%S.%3N')' AS d_caricamento FROM " + databaseName + "." + tableName +" t WHERE 1=1 and \\$CONDITIONS\"\n"
    }
    sqoopConfString += "lastValue=${2}\n"
    sqoopConfString += "driver=mssql-jdbc-9.2.1.jre8.jar\n"
    sqoopConfString += "classDriver=com.microsoft.sqlserver.jdbc.SQLServerDriver\n"
    sqoopConfString += "fetchSize=10000\n"
    // TODO numMappers a 4?
    if(ingestionMode == "FULL")
      sqoopConfString += "numMappers=1\n" // 1 for DELTA_DATE and 4 for FULL
    else sqoopConfString += "numMappers=1\n"
    sqoopConfString += "mapColumnJava=" + populatesFieldMapColumnsJava(DDLToList)
    if (ingestionMode == "FULL") sqoopConfString += "\ndeleteTargetDir=true"

    val output_path = sqoopOutputPath + sourceSystemName + "/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get( output_path + tableName.toLowerCase() +".cfg"), sqoopConfString.getBytes(StandardCharsets.UTF_8))

  }

}
