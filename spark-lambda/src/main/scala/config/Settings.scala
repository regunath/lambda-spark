package config

import com.typesafe.config.ConfigFactory

/**
  * Created by ragu on 4/30/2016.
  */
object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("clickstream")

    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val pages = weblogGen.getInt("pages")
    lazy val visitors = weblogGen.getInt("visitors")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numberOfFiles = weblogGen.getInt("number_of_files")
    lazy val sourceFileURLDocker = weblogGen.getString("sourceFileDocker")
    lazy val docker_file_path = weblogGen.getString("docker_file_path")
    lazy val sourceFileURLVagrant = weblogGen.getString("sourceFileVagrant")
    lazy val localCheckpointDir = weblogGen.getString("localCheckpointDir")
    lazy val hdfsCheckpointDir = weblogGen.getString("hdfsCheckpointDir")

  }
}
