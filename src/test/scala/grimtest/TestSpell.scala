package grimtest

import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame

/**
  * Created by caphael on 2017/9/7.
  */
class TestSpell extends Spell[DataFrame,DataFrame]{
  override def transformImpl(dat: DataFrame): DataFrame = {
    dat.groupBy("label").sum("f4")
  }
}
