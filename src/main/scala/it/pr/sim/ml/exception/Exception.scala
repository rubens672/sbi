package it.pr.sim.ml.exception

object Exception {

  sealed abstract class ApplicationException(message: String, cause: Throwable)
      extends RuntimeException(message, cause) {

    def errorCode: Int
  }

  case class ConfigurationException(message: String = null, cause: Throwable = null)
      extends ApplicationException(message, cause) {

    override val errorCode = 100
  }

  case class FormatReadException(message: String = null, cause: Throwable = null)
      extends ApplicationException(message, cause) {

    override val errorCode = 200
  }

  case class HistoryTableException(message: String = null, cause: Throwable = null)
      extends ApplicationException(message, cause) {

    override val errorCode = 300
  }

  case class FormatWriteException(message: String = null, cause: Throwable = null)
      extends ApplicationException(message, cause) {

    override val errorCode = 400
  }
}
