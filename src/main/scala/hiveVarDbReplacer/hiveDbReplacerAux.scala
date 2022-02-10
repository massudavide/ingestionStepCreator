package hiveVarDbReplacer

import auxFunctions.getListofFiles.getListOfFiles

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

object hiveDbReplacerAux{

  def hiveDbReplacer(hive_db: String, environment_datalake_hdfs_uri: String, input_path: String, output_path: String, hiveTable: String) = {
    println("\n---------------------  creating file with variable replaced for " + hiveTable + " Hive Tables --------------------- \n")
    val fileList = getListOfFiles(input_path)

    for(file <- fileList){
      val fileName = file.toString.strip().split("\\\\").last
      println(fileName)
      val src = Source.fromFile(file)
      val iterator = src.getLines
      var fileReplacedString = ""
      for(lineRaw <- iterator){
        // remove tab \t
        var line = lineRaw.replace("\t", "")
        if(line.contains("${hivevar:hive_db}")){
          line = line.replace("${hivevar:hive_db}", hive_db)
        }
        if (line.contains("${environment.datalake.hdfs.uri}")){
          line = line.replace("${environment.datalake.hdfs.uri}", environment_datalake_hdfs_uri)
        }
        fileReplacedString += line + "\n"
      }
      src.close()

      Files.createDirectories(Paths.get(output_path + hiveTable + "/"))
      Files.write(Paths.get(output_path + hiveTable + "/" + fileName), fileReplacedString.getBytes(StandardCharsets.UTF_8))
    }
  }


}
