package ingestion_step_creator_aux.DDLSynapseExternal

import ingestion_step_creator_aux.DDLSynapseExternal.synapseExteranalAux.allignment_Ext_table

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object createDDLSynapseExternal {
  def main(DDLToList: List[String], tableName: String, synapseExternalOutputPath: String) = {
    var synapseExternalString = ""

    synapseExternalString += "SET ANSI_NULLS ON\n"
    synapseExternalString += "GO\n\n"
    synapseExternalString += "SET QUOTED_IDENTIFIER ON\n"
    synapseExternalString += "GO\n\n"
    synapseExternalString += "IF OBJECT_ID('${synapse.db}.${synapse.schema}." + tableName.toLowerCase() + "_ext','ET') IS NULL\n"
    synapseExternalString += "CREATE EXTERNAL TABLE [${synapse.db}].[${synapse.schema}].["+ tableName.toLowerCase() + "_ext]\n"
    synapseExternalString += "(\n"
    synapseExternalString += allignment_Ext_table(DDLToList) + "\n"
    synapseExternalString += ")\n"
    synapseExternalString += "WITH\n(\n"
    synapseExternalString += "\tDATA_SOURCE = [${environment.synapse.managed.identity}]\n"
    synapseExternalString += "\t, LOCATION = N'integrated_data/${hive.db.integrated}/" + tableName.toLowerCase() + "'\n"
    synapseExternalString += "\t, FILE_FORMAT = [SynapseParquetFormat]\n"
    synapseExternalString += ")\nGO"

    val output_path = synapseExternalOutputPath
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + "table-" + tableName.toLowerCase() +"_ext.sql"), synapseExternalString.getBytes(StandardCharsets.UTF_8))
  }
}
