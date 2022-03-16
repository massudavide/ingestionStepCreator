package OozieProperties
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets


object createOozieProperties {

  def oozieSystemProperties(): String = {
    var sysPropString = ""

    sysPropString += "### System properties\n"
    sysPropString += "oozie.use.system.libpath=True\n"
    sysPropString += "security_enabled=True\n"
    sysPropString += "dryrun=False\n"
    sysPropString += "nameNode=${environment.hadoop.namenode.address}\n"
    sysPropString += "jobTracker=${environment.hadoop.resourcemanager.host}\n"
    sysPropString += "queueName=default\n"

    return  sysPropString
  }

  def oozieCommonProperties(tableName: String, sourceSystemName: String, processName: String, partitioningFlag: Boolean): String = {
    var commPropString = ""
    commPropString += "### Common properties\n"
    commPropString += "kapp=${snam.kapp}\n"
    commPropString += "tableName=" + tableName.toLowerCase() + "\n"
    commPropString += "tablePartitionedInCurated=" + partitioningFlag + "\n"
    commPropString += "sourceSystemName=" + sourceSystemName + "\n"
    commPropString += "processName="+ processName +"\n"
    commPropString += "startCleansingAgain=false\n"
    commPropString += "baseHdfsPath=/user/${environment.user}\n"
    commPropString += "oozieHdfsPath=${nameNode}${baseHdfsPath}/layer_raw/job_oozie\n"
    commPropString += "sqoopHdfsPath=${nameNode}${baseHdfsPath}/layer_raw/ingestion_sqoop\n"
    commPropString += "sparkHdfsPath=${nameNode}${baseHdfsPath}/layer_curated/cleansing_standardization_spark\n"
    commPropString += "compattatoreHdfsPath=${nameNode}${baseHdfsPath}/layer_curated/compattatore_spark\n"

    return  commPropString
  }

  def oozieObjectsProperties(processName: String):String = {
    var objPropString = ""

    objPropString += "### Oozie objects properties\n"
    objPropString += "startTime=${coordinator.start.time}\n"
    objPropString += "endTime=${coordinator.end.time}\n"
    objPropString += "frequenza=${coordinator.ingestion.shared.block1.frequency}\n"
    objPropString += "coordinatorPath=${oozieHdfsPath}/bin/coordinator.xml\n"
    objPropString += "coordinatorName=COORD_${kapp}_${tableName}_${sourceSystemName}${coordinator.environment.suffix}\n"

    if(processName=="dco"){
      objPropString += "workflowPath=${oozieHdfsPath}/bin/workflow-dco.xml\n"
    }
    else{
      objPropString += "workflowPath=${oozieHdfsPath}/bin/workflow-summerbi.xml\n"
    }
    objPropString += "workflowName=WF_${kapp}_${tableName}_${sourceSystemName}${coordinator.environment.suffix}\n"

    return  objPropString
  }

  def sqoopParameters(ingestionMode: String): String = {
    var sqoopParamsString = ""

    sqoopParamsString += "### Sqoop parameters\n"
    sqoopParamsString += "loadType=" + ingestionMode + "\n"
    sqoopParamsString += "filterValue=NULL\n"
    sqoopParamsString += "configFilePath=${sqoopHdfsPath}/conf/${sourceSystemName}/${tableName}.cfg\n"
    sqoopParamsString += "logFileName=${tableName}\n"
    sqoopParamsString += "folderName=${sourceSystemName}\n"

    return sqoopParamsString
  }

  def sparkParameters(): String = {
    var sparkParamsString = ""

    sparkParamsString += "### Spark parameters\n"
    sparkParamsString += "folderSistemaSorgente=${sourceSystemName}\n"
    sparkParamsString += "jsonFileName=c_${tableName}\n"
    sparkParamsString += "lastValue=NULL\n"

    return  sparkParamsString
  }

  def partitioningParameters(partitioningFlag: Boolean, dateColumn: String): String = {
    var partParamsString = ""
    partParamsString += "### Partitioning parameters\n"
    if(!partitioningFlag)
      partParamsString += "dateColumn=NULL\n"
    else
      partParamsString += "dateColumn="+ dateColumn +"\n"

    return partParamsString
  }

  def compactionParameters(): String = {
    var compactionParametersString = ""
    compactionParametersString += "### Compaction parameters\n"
    compactionParametersString += "folderSistemaSorgenteCompattatore=summer\n"
    compactionParametersString += "frequenzaCompattatore=# 19-20 L # # #\n"
    return compactionParametersString
  }

  def main(tableName: String, sourceSystemName: String, processName: String, ingestionMode: String, partitioningFlag: Boolean, dateColumn: String, oozieOutputPath: String) = {
    var propertiesToString = ""

    propertiesToString += oozieSystemProperties + "\n"
    propertiesToString += oozieCommonProperties(tableName, sourceSystemName, processName, partitioningFlag) + "\n"
    propertiesToString += oozieObjectsProperties(processName) + "\n"
    propertiesToString += sqoopParameters(ingestionMode) + "\n"
    propertiesToString += sparkParameters + "\n"
    propertiesToString += partitioningParameters(partitioningFlag, dateColumn) + "\n"
    propertiesToString += compactionParameters()

//    val output_path = "src/main/output/src/main/resources/deploy/local/layer_raw/job_oozie/conf/" + sourceSystemName +"/"
    val output_path = oozieOutputPath + processName + "/" + sourceSystemName + "/"
    // create directory if it does not exists
    Files.createDirectories(Paths.get(output_path))

    Files.write(Paths.get(output_path + tableName.toLowerCase() +".properties"), propertiesToString.getBytes(StandardCharsets.UTF_8))
  }
}