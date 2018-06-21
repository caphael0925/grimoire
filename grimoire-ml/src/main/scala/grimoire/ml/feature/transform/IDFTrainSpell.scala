package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.Implicits._
import grimoire.ml.configuration.param.HasMinDocFreq
import grimoire.transform.Spell
import org.apache.spark.ml.feature.{IDF, IDFModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/2/9.
  */
class IDFTrainSpell extends Spell[DataFrame,IDFModel]
  with HasInputCol with HasOutputCol with HasMinDocFreq{
  override def transformImpl(dat: DataFrame): IDFModel = {
    val idf = new IDF().setInputCol($(inputCol)).setOutputCol($(outputCol)).setMinDocFreq($(minDocFreq))
    idf.fit(dat)
  }
}

object IDFTrainSpell {

  def apply(json: String): IDFTrainSpell = new IDFTrainSpell().parseJson(json)
}