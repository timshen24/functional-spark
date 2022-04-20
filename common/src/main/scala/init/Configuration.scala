package init

import com.typesafe.config.{Config, ConfigFactory}

object Configuration {
  def config(env: String): Config = ConfigFactory.load.getConfig(env)
}