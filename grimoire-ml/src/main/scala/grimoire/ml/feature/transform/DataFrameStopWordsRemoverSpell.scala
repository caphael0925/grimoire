package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.HasCaseSensitive
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

import scala.io.Source

/**
  * Created by sjc505 on 17-6-19.
  */
class DataFrameStopWordsRemoverSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasCaseSensitive{

  val remover = new StopWordsRemover()


  override def setup(dat: DataFrame): Boolean = {

    val stopwordList = Source.fromFile("data/stopword.txt").getLines().toArray
   // val stopwordList:Array[String] = Array[String](stopwordFile)

    remover
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setStopWords(stopwordList)
      .setCaseSensitive($(caseSensitive))
    super.setup(dat)
  }


  override def transformImpl(dat: DataFrame): DataFrame = {
    remover.transform(dat)
  }

}

object DataFrameStopWordsRemoverSpell{
  def apply(json: JsValue): DataFrameStopWordsRemoverSpell =
    new DataFrameStopWordsRemoverSpell().parseJson(json)
}