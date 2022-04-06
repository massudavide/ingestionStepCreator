package ingestion_step_creator_aux.historizationConfiguration

import ingestion_step_creator_aux.historizationConfiguration.historizationConfigurationAux.{getColumnsName, getPrimaryKeys}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object createHistorizationConfiguration {
  def main(DDLToList: List[String], tableName: String, checkColumn: String, sourceSystemName: String,
           historizationFlag: Boolean, historization_columns: String, ingestionMode: String,
           POSSIBLE_PHYSICAL_DELETES: Boolean, HISTORICIZATION_ORDERING_COLUMN: String, historizationOutputPath: String) = {
    var historConfigString = ""

    historConfigString += "PRIMARY_KEY_COLUMNS=" + getPrimaryKeys(DDLToList) + "\n"
    historConfigString += "ORDERING_COLUMNS=\"d_caricamento\"\n"
    if(historizationFlag) {
      historConfigString += "HISTORICIZATION_COLUMNS=\"" + historization_columns + "\"\n"
      historConfigString += "HISTORICIZATION_ORDERING_COLUMN=\"" + HISTORICIZATION_ORDERING_COLUMN + "\"\n"
    }
    historConfigString += "OUTPUT_COLUMNS=" + getColumnsName(DDLToList)
    historConfigString += "\nPOSSIBLE_PHYSICAL_DELETES=" + POSSIBLE_PHYSICAL_DELETES + "\n"


    val output_path = historizationOutputPath + sourceSystemName + "/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + tableName.toLowerCase() +".cfg"), historConfigString.getBytes(StandardCharsets.UTF_8))
  }
}
