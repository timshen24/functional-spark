import sbt._

object Dependencies {
//  val kindProjector = "org.typelevel"     %% "kind-projector"  % "0.13.1" cross CrossVersion.full
  // Versions
  lazy val sparkVersion = "3.2.1"

  val deps = Seq(
    "org.apache.spark"       %% "spark-sql"                        % sparkVersion             % "provided",
    "org.apache.spark"       %% "spark-core"                       % sparkVersion             % "provided",
    "org.apache.spark"       %% "spark-core"                       % sparkVersion             % "provided",
    "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % Test)

}
