package DDLSynapseInternal

object tables_alligment_Int {

  def allignment_Int_table(DDLToList: List[String], historizationFlag: Boolean, tableName: String): String = {
    var allignmentTableString = ""

    if (historizationFlag) allignmentTableString += "\tid_sk INT IDENTITY(1,1) NOT NULL,\n"

    for (line <- DDLToList) {
      val splittedLine = line.strip().split(" ")
      if (line.contains("CONSTRAINT")) {
        if(line.contains("PRIMARY KEY")){
          if (historizationFlag) {
            allignmentTableString += "\td_caricamento datetime NULL,\n"
            allignmentTableString += "\tdata_inizio_validita datetime NULL,\n"
            allignmentTableString += "\tdata_fine_validita datetime NULL,\n"
            allignmentTableString += "\tis_current bit NULL -- ,\n"
          }
          else allignmentTableString += "\t" + "d_caricamento datetime NULL -- ,\n"

          allignmentTableString += "-- "
          var replacedLine = line
            .replace(splittedLine(1), "PK_" + tableName.toLowerCase())

          if(splittedLine(splittedLine.length-1) contains("),")){ replacedLine = replacedLine.replace("),", ")")}

          allignmentTableString += replacedLine + " NOT ENFORCED"
        }
      }
      else {
        val replacedLine = line
          .replace(splittedLine(0), splittedLine(0).toLowerCase())
        if(!replacedLine.startsWith("\t"))
          allignmentTableString += "\t"
        allignmentTableString += replacedLine + "\n"
      }
    }
    return allignmentTableString
  }

}
