package auxFunctions

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object getListofFiles {

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getUniqueNameFile(dir: String): ListBuffer[String] = {
    val filesInDirectory: List[File] = getListOfFiles(dir)
    var filesToList: ListBuffer[String] = new ListBuffer[String]()

    for (file <- filesInDirectory){
      val fileNameToList = file.toString.strip().split("\\\\")
      val fileNameWithExtension = fileNameToList(fileNameToList.length-1)

      val getOnlyNameWithoutExtensionRegex = new Regex("""\w+""")
      val fileName = getOnlyNameWithoutExtensionRegex.findFirstMatchIn(fileNameWithExtension) match {
        case Some(x) => x.toString()
      }
      filesToList += fileName
    }
    val filesToListDistinct = filesToList.distinct
    return filesToListDistinct
  }

}
