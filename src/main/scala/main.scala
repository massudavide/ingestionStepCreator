import DDLHiveCurated.createDDLHiveCurated
import DDLHiveIntegrated.createDDLHiveIntegrated
import DDLSynapseExternal.createDDLSynapseExternal
import DDLSynapseInternal.createDDLSynapseInternal
import DDLhiveRaw.createDDLHiveRaw
import OozieProperties.createOozieProperties
import auxFunctions.getDDLList.createDDLList
import auxFunctions.getListofFiles.getUniqueNameFile
import cleansingStandardizationSpark.CreateCleansingStandardizationSpark
import historizationConfiguration.createHistorizationConfiguration
import sqoopConfigFile.createSqoopConfigFile

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ListBuffer


object main {

  def filesCreator(fileName: String, inputPath: String) = {
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


    createOozieProperties.main(tableName, sourceSystemName, ingestionMode, partitioningFlag, dateColumn)
    createSqoopConfigFile.main(DDLToList, databaseName, sourceSystemName, tableName, checkColumn, ingestionMode, deleteTargetDir)
    CreateCleansingStandardizationSpark.main(DDLToList, tableName, checkColumn, sourceSystemName, ingestionMode)
    createHistorizationConfiguration.main(DDLToList, tableName, checkColumn, sourceSystemName, historizationFlag, historization_columns, ingestionMode, POSSIBLE_PHYSICAL_DELETES, HISTORICIZATION_ORDERING_COLUMN)
    createDDLHiveRaw.main(DDLToList, tableName)
    createDDLHiveCurated.main(DDLToList, tableName, partitioningFlag, curatedPartitioningColumn)
    createDDLHiveIntegrated.main(DDLToList, tableName)
    createDDLSynapseExternal.main(DDLToList, tableName)
    createDDLSynapseInternal.main(DDLToList, tableName, historizationFlag)
  }

  def createFiles(listOfFiles: ListBuffer[String], inputPath: String) = {
    if(listOfFiles.isEmpty) println("The directory is empty")
    else{
      for(fileName <- listOfFiles){
        println("working on: " + fileName)
        filesCreator(fileName, inputPath)
      }
      println("Done!")
    }
  }

  def main(args: Array[String]) = {

    val inputPath = "src/main/InputFolder/"

    val listOfFiles: ListBuffer[String] = getUniqueNameFile(inputPath)

    createFiles(listOfFiles, inputPath)

    println("\n---------------------")
    println("Attention!\nPlease check the generated files before using them.")

  }

}
