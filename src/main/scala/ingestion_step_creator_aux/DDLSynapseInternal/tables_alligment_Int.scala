package ingestion_step_creator_aux.DDLSynapseInternal

import ingestion_step_creator_aux.DDLSynapseExternal.synapseExteranalAux.{removeDefault, removeIdentity}
import ingestion_step_creator_aux.auxFunctions.regexAux.getContentInRoundBracket

object tables_alligment_Int {

  def allignment_Int_table(DDLToList: List[String], historizationFlag: Boolean, tableName: String): String = {
    var allignmentTableString = ""

    if (historizationFlag) allignmentTableString += "\tid_sk INT IDENTITY(1,1) NOT NULL,\n"

    for (lineRaw <- DDLToList) {
      var splittedLine = lineRaw.strip().split(" ")
      if (lineRaw.contains("CONSTRAINT")) {
        if(lineRaw.contains("PRIMARY KEY")){
          if (historizationFlag) {
            allignmentTableString += "\td_caricamento datetime NULL,\n"
            allignmentTableString += "\tdata_inizio_validita datetime NULL,\n"
            allignmentTableString += "\tdata_fine_validita datetime NULL,\n"
            allignmentTableString += "\tis_current bit NULL -- ,\n"
          }
          else allignmentTableString += "\t" + "d_caricamento datetime NULL -- ,\n"

          allignmentTableString += "-- "
          var replacedLine = lineRaw
            .replace(splittedLine(1), "PK_" + tableName.toLowerCase())

          if(splittedLine(splittedLine.length-1) contains("),")){ replacedLine = replacedLine.replace("),", ")")}

          allignmentTableString += replacedLine + " NOT ENFORCED"
        }
      }
      else {
        var replacedLine = lineRaw

        // remove IDENTITY from raw
        if(replacedLine.contains("IDENTITY")){
          val identity = removeIdentity(replacedLine, splittedLine)
          replacedLine = identity._1
          splittedLine = identity._2
        }

        // remove DEAFAULT from raw
        if(replacedLine.contains("DEFAULT")){
          val default = removeDefault(replacedLine, splittedLine)
          replacedLine = default._1
          splittedLine = default._2
        }

        // TODO datetime2 va bene?
        // datetime2 to datetime
//        if(splittedLine(1).startsWith("datetime")) {
//          replacedLine = lineRaw.replace(splittedLine(1), "datetime")
//        }

        // varchar(MAX) to varchar(8000)
        if(replacedLine.contains("varchar(MAX)")){
          replacedLine = replacedLine.replace("varchar(MAX)", "varchar(8000)")
          splittedLine = replacedLine.strip().split(" ")
        }

        // varbinary(MAX) to varbinary(8000)
        if(replacedLine.contains("varbinary(MAX)")){
          replacedLine = replacedLine.replace("varbinary(MAX)", "varbinary(8000)")
          splittedLine = replacedLine.strip().split(" ")
        }

        replacedLine = replacedLine
          .replace(splittedLine(0), splittedLine(0).toLowerCase())

        if(!replacedLine.startsWith("\t"))
          allignmentTableString += "\t"
        allignmentTableString += replacedLine + "\n"
      }
    }
    return allignmentTableString
  }

}
