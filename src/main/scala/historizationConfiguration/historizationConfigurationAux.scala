package historizationConfiguration

import scala.util.matching.Regex

object historizationConfigurationAux {
  def getPrimaryKeys(DDLToList: List[String]): String = {
    for (line <- DDLToList) {
      if (line.contains("CONSTRAINT")) {
        val pattern = new Regex("""\((.*?)\)""")
        var splitValueRegex = pattern findFirstMatchIn line
        return "\"" + splitValueRegex.mkString.dropRight(1).substring(1) + "\""
      }
    }
    return "ATTENZIONE! Nessuna riga contiene la parola \"CONSTRAINT\" o qualcosa è andato storto"
  }

//  def getColumnsName(DDLToList: List[String]): String = {
//    var cols = "\""
//    for (i <- 0 until DDLToList.length - 1) {
//      val col_name = DDLToList(i).strip().split(" ")(0)
//      cols += col_name + ","
//    }
//    cols = cols.dropRight(1) + ",d_caricamento\""
//    return cols
//  }
  def getColumnsName(DDLToList: List[String]): String = {
    var cols = "\""
    for (line <- DDLToList) {
      if(!line.contains("CONSTRAINT")){
        val col_name = line.strip().split(" ")(0)
        cols += col_name + ","
      }
    }
    cols += "d_caricamento\""
    return cols
  }

}
