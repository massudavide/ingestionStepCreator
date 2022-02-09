package DDLHiveIntegrated

import DDLHiveCurated.hiveCuratedAux.curated_hive_table

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object createDDLHiveIntegrated {
  def main(DDLToList: List[String], tableName: String) = {
    var hiveIntegratedString = ""

    hiveIntegratedString += "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hive_db}." + tableName.toLowerCase() + "\n"
    hiveIntegratedString += "(\n"
    hiveIntegratedString += curated_hive_table(DDLToList) + "\n"
    hiveIntegratedString += ")\n"
    hiveIntegratedString += "STORED AS PARQUET\n"
    hiveIntegratedString += "LOCATION '${environment.datalake.hdfs.uri}/integrated_data/${hivevar:hive_db}/" + tableName.toLowerCase() + "'\n"
    hiveIntegratedString += "TBLPROPERTIES ('transactional'='false')\n"
    hiveIntegratedString += ";\n\n"
    hiveIntegratedString += "ALTER TABLE ${hivevar:hive_db}." + tableName.toLowerCase() + " SET tblproperties('EXTERNAL' = 'FALSE')\n"
    hiveIntegratedString += ";"

    val output_path = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/hive/integrated/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + "table-" + tableName.toLowerCase() +".hql"), hiveIntegratedString.getBytes(StandardCharsets.UTF_8))
  }
}
