import DDLHiveCurated.createDDLHiveCurated
import DDLHiveIntegrated.createDDLHiveIntegrated
import DDLSynapseExternal.createDDLSynapseExternal
import DDLSynapseInternal.createDDLSynapseInternal
import DDLhiveRaw.createDDLHiveRaw
import OozieProperties.createOozieProperties
import auxFunctions.getDDLList.createDDLList
import auxFunctions.getListofFiles.getUniqueNameFile
import auxFunctions.manageAccent
import cleansingStandardizationSpark.CreateCleansingStandardizationSpark
import historizationConfiguration.createHistorizationConfiguration
import sqoopConfigFile.createSqoopConfigFile

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import hiveVarDbReplacer.hiveDbVarReplacer

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
    val deleteTargetDir: Boolean = config.getString("deleteTargetDir").toBoolean

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


    createOozieProperties.main(tableName, sourceSystemName, ingestionMode, partitioningFlag, dateColumn, oozieOutputPath)
    createSqoopConfigFile.main(DDLToList, databaseName, sourceSystemName, tableName, checkColumn, ingestionMode, deleteTargetDir, sqoopOutputPath)
    CreateCleansingStandardizationSpark.main(DDLToList, tableName, checkColumn, sourceSystemName, ingestionMode, cleansingOutputPath)
    createHistorizationConfiguration.main(DDLToList, tableName, checkColumn, sourceSystemName, historizationFlag,
      historization_columns, ingestionMode, POSSIBLE_PHYSICAL_DELETES, HISTORICIZATION_ORDERING_COLUMN, historizationOutputPath)
    createDDLHiveRaw.main(DDLToList, tableName, rawHiveOutputPath)
    createDDLHiveCurated.main(DDLToList, tableName, partitioningFlag, curatedPartitioningColumn, curatedHiveOutputPath)
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

    val inputPath = "src/main/InputFolder/"

    val oozieOutputPath = "src/main/output/src/main/resources/deploy/local/layer_raw/job_oozie/conf/"
    val sqoopOutputPath = "src/main/output/src/main/resources/deploy/hdfs/layer_raw/ingestion_sqoop/conf/"
    val cleansingOutputPath = "src/main/output/src/main/resources/deploy/hdfs/layer_curated/cleansing_standardization_spark/conf/"
    val historizationOutputPath = "src/main/output/src/main/resources/deploy/hdfs/summerbi/conf/"
    val rawHiveOutputPath = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/hive/raw/"
    val curatedHiveOutputPath = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/hive/curated/"
    val integratedHiveOutputPath = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/hive/integrated/"
    val synapseExternalOutputPath = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/synapse/tables_allignment/"
    val synapseInternalOutputPath = "src/main/output/src/main/resources/deploy/local/summerbi/ddl/synapse/tables_allignment/"



    val listOfFiles: ListBuffer[String] = getUniqueNameFile(inputPath)
    if (listOfFiles.isEmpty) {
      println("The directory is empty\nPath: " + inputPath)
      break
    }

    createFiles(listOfFiles, inputPath, oozieOutputPath, sqoopOutputPath, cleansingOutputPath,
      historizationOutputPath, rawHiveOutputPath, curatedHiveOutputPath, integratedHiveOutputPath,
      synapseExternalOutputPath, synapseInternalOutputPath)

    manageAccent.main(rawHiveOutputPath, curatedHiveOutputPath, integratedHiveOutputPath)

    hiveDbVarReplacer.main()

    println("\n---------------------\n")
    println("Attention!\nPlease check the generated files before using them.")

  }

}
