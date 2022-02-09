package DDLSynapseInternal

import DDLSynapseInternal.tables_alligment_Int.allignment_Int_table

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}


object createDDLSynapseInternal {
  def main(DDLToList: List[String], tableName: String, historizationFlag: Boolean) = {
    var synapseInternalString = ""

    synapseInternalString += "SET ANSI_NULLS ON\nGO\n\n"
    synapseInternalString += "SET QUOTED_IDENTIFIER ON\nGO\n\n"
    synapseInternalString += "IF OBJECT_ID('${synapse.db}.${synapse.schema}." + tableName.toLowerCase() + "','U') IS NULL\n"
    synapseInternalString += "CREATE TABLE [${synapse.db}].[${synapse.schema}].[" + tableName.toLowerCase() +"]\n"
    synapseInternalString += "(\n"
    synapseInternalString += allignment_Int_table(DDLToList, historizationFlag, tableName) + "\n"
    synapseInternalString += ")\n"
    synapseInternalString += "WITH\n(\n"
    synapseInternalString += "\tDISTRIBUTION = ROUND_ROBIN,\n"
    synapseInternalString += "\tCLUSTERED COLUMNSTORE INDEX\n"
    synapseInternalString += ")\nGO"

    val output_path = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/synapse/tables_allignment/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + "table-" + tableName.toLowerCase() +".sql"), synapseInternalString.getBytes(StandardCharsets.UTF_8))
  }
}
