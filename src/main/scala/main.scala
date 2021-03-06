import ingestion_step_creator_aux.DDLHiveCurated.createDDLHiveCurated
import ingestion_step_creator_aux.DDLHiveIntegrated.createDDLHiveIntegrated
import ingestion_step_creator_aux.DDLSynapseExternal.createDDLSynapseExternal
import ingestion_step_creator_aux.DDLSynapseInternal.createDDLSynapseInternal
import ingestion_step_creator_aux.DDLhiveRaw.createDDLHiveRaw
import ingestion_step_creator_aux.OozieProperties.createOozieProperties
import ingestion_step_creator_aux.auxFunctions.getDDLList.createDDLList
import ingestion_step_creator_aux.auxFunctions.getListofFiles.getUniqueNameFile
import ingestion_step_creator_aux.auxFunctions.manageAccent
import ingestion_step_creator_aux.cleansingStandardizationSpark.createCleansingStandardizationSpark
import ingestion_step_creator_aux.historizationConfiguration.createHistorizationConfiguration
import ingestion_step_creator_aux.sqoopConfigFile.createSqoopConfigFile

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import ingestion_step_creator_aux.hiveVarDbReplacer.hiveDbVarReplacer

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break


object main {

  def filesCreator(fileName: String, inputPath: String,
                   oozieOutputPath: String,
                   sqoopOutputPath: String,
                   cleansingOutputPath: String,
                   historizationOutputPath: String,
                   rawHiveOutputPath: String,
                   curatedHiveOutputPath: String,
                   integratedHiveOutputPath: String,
                   synapseExternalOutputPath: String,
                   synapseInternalOutputPath: String) = {
    val myConfigFile = new File(inputPath + fileName +".conf")
    val config = ConfigFactory.parseFile(myConfigFile)

    val filePath = inputPath + fileName + ".txt"

    // es databaseName: summer_pool.dbo
    val databaseName: String = config.getString("databaseName")
    val tableName: String = config.getString("tableName")
    val sourceSystemName: String = config.getString("sourceSystemName")

    // ingestionMode: DELTA_DATE or FULL
    val ingestionMode: String = config.getString("ingestionMode")
    val checkColumn: String = config.getString("checkColumn")
    // if ingestionMode is FULL set deleteTargetDil = true/false
    // val deleteTargetDir: Boolean = config.getString("deleteTargetDir").toBoolean

    // if some fields need to be historicized set historizationFlag to true
    val historizationFlag: Boolean = config.getString("historizationFlag").toBoolean
    // es: val historization_columns = "s_cliente,x_rag_soc,x_rag_soc2,c_partita_iva"
    val historization_columns: String = config.getString("historization_columns")

    // if HISTORICIZATION_ORDERING_COLUMN is "" it will be equal to checkColumn
    var HISTORICIZATION_ORDERING_COLUMN: String = config.getString("HISTORICIZATION_ORDERING_COLUMN")
    if(HISTORICIZATION_ORDERING_COLUMN == "") HISTORICIZATION_ORDERING_COLUMN = checkColumn


    val POSSIBLE_PHYSICAL_DELETES: Boolean = config.getString("POSSIBLE_PHYSICAL_DELETES").toBoolean

    // Partitioning parameters
    val partitioningFlag: Boolean = config.getString("partitioningFlag").toBoolean
    val dateColumn: String = config.getString("dateColumn")
    val curatedPartitioningColumn: String = config.getString("curatedPartitioningColumn")


    val DDLToList = createDDLList(filePath)

    // Oozie Prop for dco
    createOozieProperties.main(tableName, sourceSystemName, "dco", ingestionMode, partitioningFlag, dateColumn, oozieOutputPath)
    // Oozie Prop for summerbi
    createOozieProperties.main(tableName, sourceSystemName, "summerbi", ingestionMode, partitioningFlag, dateColumn, oozieOutputPath)

    createSqoopConfigFile.main(DDLToList, databaseName, sourceSystemName, tableName, checkColumn, ingestionMode, sqoopOutputPath)

    // cleansing Standard Spark for dco
    createCleansingStandardizationSpark.main(DDLToList, tableName, checkColumn, sourceSystemName, "dco", ingestionMode, cleansingOutputPath)
    // cleansing Standard Spark for summerbi
    // createCleansingStandardizationSpark.main(DDLToList, tableName, checkColumn, sourceSystemName, "summerbi", ingestionMode, cleansingOutputPath)


    createHistorizationConfiguration.main(DDLToList, tableName, checkColumn, sourceSystemName, historizationFlag,
      historization_columns, ingestionMode, POSSIBLE_PHYSICAL_DELETES, HISTORICIZATION_ORDERING_COLUMN, historizationOutputPath)

    createDDLHiveRaw.main(DDLToList, tableName, rawHiveOutputPath)
    // create tmp raw
    createDDLHiveRaw.main(DDLToList, tableName, rawHiveOutputPath, ingestionMode)

    createDDLHiveIntegrated.main(DDLToList, tableName, integratedHiveOutputPath)
    createDDLSynapseExternal.main(DDLToList, tableName, synapseExternalOutputPath)
    createDDLSynapseInternal.main(DDLToList, tableName, historizationFlag, synapseInternalOutputPath)

  }

  def createFiles(listOfFiles: ListBuffer[String],
                  inputPath: String,
                  oozieOutputPath: String,
                  sqoopOutputPath: String,
                  cleansingOutputPath: String,
                  historizationOutputPath: String,
                  rawHiveOutputPath: String,
                  curatedHiveOutputPath: String,
                  integratedHiveOutputPath: String,
                  synapseExternalOutputPath: String,
                  synapseInternalOutputPath: String) = {


    println("--------------------- creating Files ---------------------\n")
    for(fileName <- listOfFiles){
      println(fileName)
      filesCreator(fileName, inputPath, oozieOutputPath, sqoopOutputPath, cleansingOutputPath,
        historizationOutputPath, rawHiveOutputPath, curatedHiveOutputPath, integratedHiveOutputPath,
        synapseExternalOutputPath, synapseInternalOutputPath)
    }
  }

  def main(args: Array[String]) = {

    val inputPath = "src/main/input/"
    val outputPath = "src/main/output/"

    val oozieOutputPath = outputPath + "src/main/resources/deploy/local/layer_raw/job_oozie/conf/"
    val sqoopOutputPath = outputPath + "src/main/resources/deploy/hdfs/layer_raw/ingestion_sqoop/conf/"
    val cleansingOutputPath = outputPath + "src/main/resources/deploy/hdfs/layer_curated/cleansing_standardization_spark/conf/"
    val historizationOutputPath = outputPath + "src/main/resources/deploy/hdfs/summerbi/conf/"
    val rawHiveOutputPath = outputPath + "src/main/resources/deploy/local/summerbi/ddl/hive/raw/"
    val curatedHiveOutputPath = outputPath + "src/main/resources/deploy/local/summerbi/ddl/hive/curated/"
    val integratedHiveOutputPath = outputPath + "src/main/resources/deploy/local/summerbi/ddl/hive/integrated/"
    val synapseExternalOutputPath = outputPath + "src/main/resources/deploy/local/summerbi/ddl/synapse/tables_allignment/"
    val synapseInternalOutputPath = outputPath + "src/main/resources/deploy/local/summerbi/ddl/synapse/tables_allignment/"

    val hiveReplacerOutputPath = outputPath + "hiveValueReplaced/"

    val listOfFiles: ListBuffer[String] = getUniqueNameFile(inputPath)
    if (listOfFiles.isEmpty) {
      println("The directory is empty\nPath: " + inputPath)
      break
    }

    createFiles(listOfFiles, inputPath, oozieOutputPath, sqoopOutputPath, cleansingOutputPath,
      historizationOutputPath, rawHiveOutputPath, curatedHiveOutputPath, integratedHiveOutputPath,
      synapseExternalOutputPath, synapseInternalOutputPath)

    manageAccent.main(rawHiveOutputPath, curatedHiveOutputPath, integratedHiveOutputPath)

    hiveDbVarReplacer.main(outputPath, hiveReplacerOutputPath)

    println("\n---------------------\n")
    println("Attention!\nPlease check the generated files before using them.")

  }

}
