package auxFunctions

import auxFunctions.getDDLList.createDDLList
import auxFunctions.getListofFiles.getListOfFiles
import sun.font.TrueTypeFont

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

object manageAccent {

  def manageAccentInHiveTable(input_path: String, hiveTable: String): Unit ={
    val fileList = getListOfFiles(input_path)
    manageFile(fileList, input_path, hiveTable)
  }

  def checkAndManageAccent(fileIterator: Iterator[String]): String = {
    var fileReplacedString = ""
    for(lineRaw <- fileIterator){
      val splittedLine = lineRaw.strip().split(" ")
      var changedLine = lineRaw
      for(word <- splittedLine){
        val changedWord = findAndManageAccent(word)
        if(changedWord._2)
          changedLine = changedLine.replace(word, changedWord._1)
      }
      fileReplacedString += changedLine + "\n"
    }
    fileReplacedString
  }

  def checkIfTableContainsAccent(filePath: String): Boolean = {
    val DDLToList = createDDLList(filePath)
    for(line <- DDLToList){
      val words = line.strip().split(" ")
      if(accent(words(0))) return true
    }
    return false
  }

  def manageFile(fileList: List[File], input_path: String, hiveTable: String): Unit = {
    for(file <- fileList){
      val fileName = file.toString.strip().split("\\\\").last
      val src = Source.fromFile(file)
      val iterator = src.getLines

      if(checkIfTableContainsAccent(file.toString)){
        println("managing accent for " + fileName +" in " + hiveTable + " Hive table")
        val fileReplacedString = checkAndManageAccent(iterator)
        Files.createDirectories(Paths.get(input_path))
        Files.write(Paths.get(input_path + fileName), fileReplacedString.getBytes(StandardCharsets.UTF_8))
      }
      src.close()
    }
  }

  def main(rawHiveInputPath: String, curatedHiveInputPath: String, integratedHiveInputPath: String) = {

    println("\n--------------------- checking accent in Hive tables ---------------------\n")

    manageAccentInHiveTable(rawHiveInputPath, "raw")

    manageAccentInHiveTable(curatedHiveInputPath, "curated")

    manageAccentInHiveTable(integratedHiveInputPath, "integrated")
  }

  def accent(word: String) = {
    word.contains("\u00e0") || word.contains("\u00c0") ||   // à À
    word.contains("\u00e8") || word.contains("\u00c8") ||   // è È
    word.contains("\u00e9") || word.contains("\u00c9") ||   // é É
    word.contains("\u00ec") || word.contains("\u00cc") ||   // ì Ì
    word.contains("\u00f2") || word.contains("\u00d2") ||   // ò Ì
    word.contains("\u00f9") || word.contains("\u00d9")      // ù Ù
  }

  def findAndManageAccent(word: String): (String, Boolean) = {
    // if you need to find unicode of special char
    // https://www.fileformat.info/index.htm
    if (accent(word))
      return ("\u0060" + word + "\u0060", true)
    else (word, false)
  }
}
