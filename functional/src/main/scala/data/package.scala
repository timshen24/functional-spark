import com.typesafe.config._

package object data {
  def config(env: String): Config = ConfigFactory.load.getConfig(env)

  val renderedPrint: String => Unit = str =>
    println(Console.CYAN + str + Console.RESET)
}
