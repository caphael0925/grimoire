package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.dataset.&
import grimoire.transform.Spell
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/2/9.
  */
class DataFrameTFIDFVectorizeSpell extends Spell[DataFrame & IDFModel,DataFrame] with HasInputCol with HasOutputCol{

  override def transformImpl(dat: DataFrame & IDFModel): DataFrame = {
    dat._2
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .transform(dat._1)
  }

}

object DataFrameTFIDFVectorizeSpell{

def apply(json: JsValue): DataFrameTFIDFVectorizeSpell =
  new DataFrameTFIDFVectorizeSpell().parseJson(json)

}