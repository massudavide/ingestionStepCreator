package ingestion_step_creator_aux.DDLhiveRaw

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object createDDLHiveRaw {
  def main(DDLToList: List[String], tableName: String, rawHiveOutputPath: String) = {
    var hiveRawString = ""

    hiveRawString += "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hive_db}.r_" + tableName.toLowerCase() + "\n"
    hiveRawString += "(\n"
    hiveRawString += hiveRawAux.mapIntoHiveType(DDLToList) + "\n"
    hiveRawString += ")\n"
    hiveRawString += "STORED AS PARQUET\n"
    hiveRawString += "LOCATION '${environment.datalake.hdfs.uri}/raw_data/${hivevar:hive_db}/r_" + tableName.toLowerCase() + "'\n"
    hiveRawString += "TBLPROPERTIES ('transactional'='false')\n"
    hiveRawString += ";"

    val output_path = rawHiveOutputPath
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get( output_path + "table-r_" +tableName.toLowerCase() +".hql"), hiveRawString.getBytes(StandardCharsets.UTF_8))
  }
  def main(DDLToList: List[String], tableName: String, rawHiveOutputPath: String, ingestionMode: String) = {
    var hiveRawString = ""
    if(ingestionMode.startsWith("DELTA")){
      hiveRawString += "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hive_db}.r_" + tableName.toLowerCase() + "_tmp\n"

      hiveRawString += "(\n"
      hiveRawString += hiveRawAux.mapIntoHiveType(DDLToList) + "\n"
      hiveRawString += ")\n"
      hiveRawString += "STORED AS PARQUET\n"
      hiveRawString += "LOCATION '${environment.datalake.hdfs.uri}/raw_data/${hivevar:hive_db}/r_" + tableName.toLowerCase() + "_tmp'\n"
      hiveRawString += "TBLPROPERTIES ('transactional'='false')\n"
      hiveRawString += ";"

      val output_path = rawHiveOutputPath
      // create directory if it does not exists
      Files.createDirectories(Paths.get(output_path))

      Files.write(Paths.get( output_path + "table-r_" +tableName.toLowerCase() +"_tmp.hql"), hiveRawString.getBytes(StandardCharsets.UTF_8))
    }
  }
}
