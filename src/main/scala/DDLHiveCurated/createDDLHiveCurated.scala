package DDLHiveCurated

import DDLHiveCurated.hiveCuratedAux.curated_hive_table

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object createDDLHiveCurated {
  def main(DDLToList: List[String], tableName: String, partitioning: Boolean, curatedPartitioningColumn: String) = {
    var hiveCuratedString = ""

    hiveCuratedString += "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hive_db}.c_" + tableName.toLowerCase() + "\n"
    hiveCuratedString += "(\n"
    hiveCuratedString += curated_hive_table(DDLToList) + "\n"
    hiveCuratedString += ")\n"
    if(partitioning){
      hiveCuratedString += "PARTITIONED BY\n(\n\t" + curatedPartitioningColumn + "\n)\n"
    }
    hiveCuratedString += "STORED AS PARQUET\n"
    hiveCuratedString += "LOCATION '${environment.datalake.hdfs.uri}/curated_data/${hivevar:hive_db}/c_" + tableName.toLowerCase() + "'\n"
    hiveCuratedString += "TBLPROPERTIES ('transactional'='false')\n"
    hiveCuratedString += ";\n\n"
    hiveCuratedString += "ALTER TABLE ${hivevar:hive_db}.c_" + tableName.toLowerCase() + " SET tblproperties('EXTERNAL' = 'FALSE')\n;"

    val output_path = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/hive/curated/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + "table-c_" + tableName.toLowerCase() +".hql"), hiveCuratedString.getBytes(StandardCharsets.UTF_8))
  }
}
