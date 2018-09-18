package com.lucidworks.spark

import org.slf4j.{LoggerFactory, Logger => Underlying}


final class Logger private (val underlying: Underlying) extends Serializable {

  def info(msg: String): Unit = if (underlying.isInfoEnabled) underlying.info(msg)

  def info(msg: String, t: Throwable): Unit = if (underlying.isInfoEnabled) underlying.info(msg, t)

  def info(msg: String, args: Any*): Unit = if (underlying.isInfoEnabled) underlying.info(msg, args)

  def debug(msg: String): Unit = if (underlying.isDebugEnabled) underlying.debug(msg)

  def debug(msg: String, t: Throwable): Unit = if (underlying.isDebugEnabled) underlying.debug(msg, t)

  def debug(msg: String, args: Any*): Unit = if (underlying.isDebugEnabled) underlying.debug(msg, args)

  def trace(msg: String): Unit = if (underlying.isTraceEnabled) underlying.trace(msg)

  def trace(msg: String, t: Throwable): Unit = if (underlying.isTraceEnabled) underlying.trace(msg, t)

  def trace(msg: String, args: Any*): Unit = if (underlying.isTraceEnabled) underlying.trace(msg, args)

  def error(msg: String): Unit = if (underlying.isErrorEnabled) underlying.error(msg)

  def error(msg: String, t: Throwable): Unit = if (underlying.isErrorEnabled) underlying.error(msg, t)

  def error(msg: String, args: Any*): Unit = if (underlying.isErrorEnabled) underlying.error(msg, args)

  def warn(msg: String): Unit = if (underlying.isWarnEnabled) underlying.warn(msg)

  def warn(msg: String, t: Throwable): Unit = if (underlying.isWarnEnabled) underlying.warn(msg, t)

  def warn(msg: String, args: Any*): Unit = if (underlying.isWarnEnabled) underlying.warn(msg, args)

}

/**
  * Companion for [[Logger]], providing a factory for [[Logger]]s.
  */
object Logger {

  /**
    * Create a [[Logger]] wrapping the given underlying `org.slf4j.Logger`.
    */
  def apply(underlying: Underlying): Logger =
    new Logger(underlying)

  /**
    * Create a [[Logger]] wrapping the created underlying `org.slf4j.Logger`.
    */
  def apply(clazz: Class[_]): Logger =
    new Logger(LoggerFactory.getLogger(clazz.getName))
}


trait LazyLogging {
  protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
}

trait StrictLogging {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
}