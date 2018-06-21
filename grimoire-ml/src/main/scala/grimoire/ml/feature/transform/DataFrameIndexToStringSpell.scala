package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasLabels
import grimoire.spark.transform.dataframe.DataFrameSpell
/**
  * Created by sjc505 on 17-6-21.
  */

class DataFrameIndexToStringSpell extends DataFrameSpell with HasInputCol with HasOutputCol with HasLabels{

  val converter = new IndexToString()

  override protected def setup(dat: DataFrame): Boolean = {
    converter
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setLabels($(labels).toArray)
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    converter.transform(dat)
  }
}

object DataFrameIndexToStringSpell{
  def apply(json: JsValue="""{}"""): DataFrameIndexToStringSpell =
    new DataFrameIndexToStringSpell().parseJson(json)
}