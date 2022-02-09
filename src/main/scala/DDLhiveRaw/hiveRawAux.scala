package DDLhiveRaw

object hiveRawAux {

  def mapIntoHiveType(DDLToList: List[String]): String = {
    // create an empty string
    var nomeColTipoStr = ""
    for (line <- DDLToList) {
      // we do not need last element of the list
      if (!line.contains("CONSTRAINT")) {
        val splittedLine = line.strip().split(" ")

        nomeColTipoStr += "\t" + splittedLine(0).toLowerCase()
        if (splittedLine(1) == "int") {
          nomeColTipoStr += " INT,\n"
        }
        if (splittedLine(1) == "smallint") {
          nomeColTipoStr += " SMALLINT,\n"
        }
        if (splittedLine(1) == ("bigint")) {
          nomeColTipoStr += " BIGINT,\n"
        }
        if (splittedLine(1).startsWith("numeric")) {
          nomeColTipoStr += " STRING,\n"
        }
        if (splittedLine(1).startsWith("decimal")) {
          nomeColTipoStr += " STRING,\n"
        }
        if (splittedLine(1) == ("float")) {
          nomeColTipoStr += " DOUBLE,\n"
        }
        if (splittedLine(1) == ("datetime")) {
          nomeColTipoStr += " STRING,\n"
        }
        if (splittedLine(1).startsWith("varchar")) {
          nomeColTipoStr += " STRING,\n"
        }
        if (splittedLine(1) == ("bit")) {
          nomeColTipoStr += " BOOLEAN,\n"
        }
      }
    }
    nomeColTipoStr += "\td_caricamento STRING"
    return nomeColTipoStr
  }
}
