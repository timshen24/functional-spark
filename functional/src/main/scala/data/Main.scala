package data

import data.etl.impl1._

object Impl1DevMain {
  def main(args: Array[String]): Unit = {
    import Impl1ETLDev._
    etl.run(null)
  }
}

object Impl1ProdMain {
  def main(args: Array[String]): Unit = {
    import Impl1ETLProd._
    etl.run(CustomRepoConfig(args(0), "baseDF.dir", "dest.path1", Impl1ETLProd.step0, transformTable1))
  }
}