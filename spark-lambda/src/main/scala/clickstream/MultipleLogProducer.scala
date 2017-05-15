package clickstream

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.util.Random

object MultipleLogProducer extends App {

  val logger:Logger = Logger.getLogger(getClass().getName())

  logger.debug("Testing....")

  // WebLog config
  val config = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to config.visitors).map("Visitor-" + _)
  val Pages = (0 to config.pages).map("Page-" + _)

  val rnd = new Random()


//  val filePath = config.filePath
  val filePath = config.docker_file_path
  val destPath = config.destPath
  val numberOfFiles = config.numberOfFiles

  for (fileCount <- 0 to numberOfFiles) {


    val fw = new FileWriter(filePath, true)

    // introduce a bit of randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(math.min(config.records, 100) - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to config.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * config.timeMultiplier)
      timestamp = System.currentTimeMillis()
      // move all this to a function
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      fw.write(line)

      if (iteration % incrementTimeEvery == 0) {
        logger.debug(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        logger.debug(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }
    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    logger.debug(s"Moving the produced data to the output location $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 5000
    logger.debug(s"Sleeping for $sleeping ms ")
    Thread sleep 5000
  }
}
