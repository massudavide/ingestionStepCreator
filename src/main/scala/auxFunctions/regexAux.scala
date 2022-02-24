package auxFunctions

import scala.util.matching.Regex

object regexAux {
  def regexValueWithoutSome(valuesRegex: Regex, value: String): String = {
    val splitValueRegex = valuesRegex findFirstMatchIn value match {
      case Some(x) => x
    }
    splitValueRegex.toString()
  }

  def getValuesInRoundBrackets(valuesInRoundBrackets: String): (Int, Int) = {
    val valuesWithoutRoundBrackets = valuesInRoundBrackets.substring(1, valuesInRoundBrackets.length - 1)
    val valuesSplitted = valuesWithoutRoundBrackets.split(",")
    if (valuesSplitted.length == 2){
      return (valuesSplitted(0).toInt, valuesSplitted(1).toInt)
    }
    else if(valuesSplitted.length == 1){
      return (valuesSplitted(0).toInt, 0)
    }
    else return (38, 0)
  }

  def getContentInRoundBracket(raw: String): String ={
    val valuesInBracketRegex = new Regex("""\((.*?)\)""")
    val content = regexValueWithoutSome(valuesInBracketRegex, raw)
    return content
  }
}
