package com.github.takezoe.predictionio.toolbox

import org.apache.predictionio.data.storage.Storage
import scala.collection.mutable
import grizzled.slf4j.Logger

private[toolbox] object Common {

  @transient lazy val logger = Logger[this.type]
  @transient lazy private val appsDb = Storage.getMetaDataApps()
  @transient lazy private val channelsDb = Storage.getMetaDataChannels()
  // Memoize app & channel name-to-ID resolution to avoid excessive storage IO
  @transient lazy val appNameToIdCache =
  mutable.Map[(String, Option[String]), (Int, Option[Int])]()

  /* throw exception if invalid app name or channel name */
  def appNameToId(appName: String, channelName: Option[String]): (Int, Option[Int]) = {
    appNameToIdCache.getOrElseUpdate((appName, channelName), {
      val appOpt = appsDb.getByName(appName)

      appOpt.map { app =>
        val channelMap: Map[String, Int] = channelsDb.getByAppid(app.id)
          .map(c => (c.name, c.id)).toMap

        val channelId: Option[Int] = channelName.map { ch =>
          if (channelMap.contains(ch)) {
            channelMap(ch)
          } else {
            logger.error(s"Invalid channel name ${ch}.")
            throw new IllegalArgumentException(s"Invalid channel name ${ch}.")
          }
        }

        appNameToIdCache((appName, channelName)) = (app.id, channelId)
        (app.id, channelId)
      }.getOrElse {
        logger.error(s"Invalid app name ${appName}")
        throw new IllegalArgumentException(s"Invalid app name ${appName}")
      }
    })
  }

  def processEnvVars(pioHome: String, variables: Seq[(String, String)]): Map[String, String] = {
    val replaceVars = mutable.Map(
      "$PIO_HOME" -> pioHome,
      "$HOME"     -> System.getProperty("user.home")
    )

    variables.map { case (key, value) =>
      val replaced = replaceVars.foldLeft(value){ case (value, (target, replacement)) =>
        value.replace(target, replacement)
      }
      replaceVars.put("$" + key, replaced)

      key -> replaced
    }.toMap
  }

}
