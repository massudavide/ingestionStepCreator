package ingestion_step_creator_aux.sqoopConfigFile
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import ingestion_step_creator_aux.sqoopConfigFile.sqoopAux.{populatesFieldMapColumnsJava, getFirstPrimaryKey}

object createSqoopConfigFile {

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
    sqoopConfString += "querySource=\"SELECT t.*, '$(date '+%Y-%m-%d %H:%M:%S.%3N')' AS d_caricamento FROM " + databaseName + "." + tableName +" t WHERE 1=1 and \\$CONDITIONS\"\n"
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
