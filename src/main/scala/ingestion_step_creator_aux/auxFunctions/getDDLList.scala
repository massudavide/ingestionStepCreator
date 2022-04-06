package ingestion_step_creator_aux.auxFunctions

import scala.collection.mutable.ListBuffer
import scala.io.Source

object getDDLList {

  def createDDLList(filePath: String): List[String] ={
    // Read the file and save it as a List
    val src = Source.fromFile(filePath)
    val DDL_Iterator = src.getLines
    val DDLToListBuffer = new ListBuffer[String]()
    for(line <- DDL_Iterator){
      if(line != ""){
        DDLToListBuffer += line
      }
    }
    src.close()
    val DDLToList = DDLToListBuffer.toList
    DDLToList
  }
}
