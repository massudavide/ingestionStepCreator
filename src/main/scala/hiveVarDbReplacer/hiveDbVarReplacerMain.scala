package hiveVarDbReplacer

import com.typesafe.config.ConfigFactory
import hiveVarDbReplacer.hiveDbReplacerAux.hiveDbReplacer

import java.io.File

object hiveDbVarReplacerMain {
  def main() = {
    val myConfigFile = new File("src/main/scala/hiveDbVarReplacer.conf")
    val config = ConfigFactory.parseFile(myConfigFile)

    val output_path = config.getString("output_path")

    val input_path = config.getString("input_path")
    val raw_input_path = input_path + "src/main/resources/deploy/local/summerbi/ddl/hive/raw/"
    val curated_input_path = input_path + "src/main/resources/deploy/local/summerbi/ddl/hive/curated"
    val integrated_input_path = input_path + "src/main/resources/deploy/local/summerbi/ddl/hive/integrated"

    val environment_datalake_hdfs_uri = config.getString("environment_datalake_hdfs_uri")

    val raw_hive_db = config.getString("raw_hive_db")
    val curated_hive_db = config.getString("curated_hive_db")
    val integrated_hive_db = config.getString("integrated_hive_db")

    // raw hive table
    hiveDbReplacer(raw_hive_db, environment_datalake_hdfs_uri, raw_input_path, output_path, "raw")

    // curated hive table
    hiveDbReplacer(curated_hive_db, environment_datalake_hdfs_uri, curated_input_path, output_path, "curated")

    // integrated hive table
    hiveDbReplacer(integrated_hive_db, environment_datalake_hdfs_uri, integrated_input_path, output_path, "integrated")
  }
}
