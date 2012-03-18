<?php

if (!class_exists("FineLog")) {

require_once(dirname(__FILE__) . "/except.IOException.php");

/**
 * Object for log messages management.
 *
 * This object provides static methods, which can be used to write messages in a centralised log file.
 * Examples:
 * <code>
 * // basic use, INFO level by default
 * FineLog::log("Log message");
 * // advanced use, specifying the criticity level
 * FineLog::log(FineLog::WARN, "Warning message");
 * // complete use, specifying the criticity level, the current file name, the current line number, and
 * // the current object and method names.
 * FineLog::fullLog(FineLog::ERROR, "Error message", __FILE__, __LINE__, __METHOD__);
 * // like the previous one, specifying the name of the caller function (non-object oriented programming)
 * FineLog::fullLog(FineLog::ERROR, "Error message", __FILE__, __LINE__, __FUNCTION__);
 * // like the previous one, without specifying method and function name (called from global execution context)
 * FineLog::fullLog(FineLog::ERROR, "Error message", __FILE__, __LINE__, "");
 * </code>
 *
 * It is possible to define the criticity threshold. Any message below this level will be ignored.
 * There is 6 criticity levels:
 * - DEBUG: debug message (lowest criticity)
 * - INFO: information message (default level)
 * - NOTE: notification; normal but significant message (default threshold)
 * - WARN: alert message; the program doesn't work as it should, but it can run anyway.
 * - ERROR: error message; the program doesn't work as it should, and it must be stopped.
 * - CRIT: critical error message; the program risks to damage its environment (filesystem or database).
 * <code>
 * // threshold definition
 * FineLog::setThreshold(FineLog::INFO);
 * // then this message will be discarded
 * FineLog::log(FineLog::DEBUG, "Debug message");
 * // but this one wil be written
 * FineLog::log(FineLog::NOTE, "Notification);
 * </code>
 *
 * It is possible to define some "log classes", which are labels used to group log messages. Each class has
 * a different log threshold.
 * <code>
 * // log thresholds init
 * $thresholds = array(
 *         "default" => FineLog::ERROR,
 *         "testing" => FineLog::DEBUG
 * );
 * FineLog::setThreshold($thresholds);
 * // this message will be discarded
 * FineLog::log(FineLog::WARN, "No class message");
 * // this one will be written
 * FineLog::log("default", FineLog::ERROR, "Default class message");
 * // this one will be written too
 * FineLog::log("testing", FineLog::CRIT, "Applicative message");
 *
 * // log classes can be used with extended logs
 * FineLog::fullLog("testing", FineLog::NOTE, "Notification", __FILE__, __LINE__, __METHOD__);
 * </code>
 * For information:
 * - Class-less messages are linked to the "default" class.
 * - If a log message is sent with a non-defined class, the message will not be written.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2007, FineMedia
 * @package	FineFS
 * @subpackage	utils
 * @version	$Id$
 */
class FineLog {
	/** Constant - debug message level (lowest priority). */
	const DEBUG = 10;
	/** Constant - information message level (default level). */
	const INFO = 20;
	/** Constant - notification message level. */
	const NOTE = 30;
	/** Constant - alert message level. */
	const WARN = 40;
	/** Constant - error message level. */
	const ERROR = 50;
	/** Constant - critical error message level. */
	const CRIT = 60;
	/** Constant - name of the default log class. */
	const DEFAULT_CLASS = "default";
	/** Path to the log file. */
	static private $_logPath = null;
	/** Current priority threshold. */
	static private $_threshold = self::NOTE;
	/** Log level labels. */
	static private $_labels = array(
		10 => "DEBUG",
		20 => "INFO ",
		30 => "NOTE ",
		40 => "WARN ",
		50 => "ERROR",
		60 => "CRIT "
	);

	/* ******************** PUBLIC METHODS ****************** */
	/**
	 * Set the log file's path.
	 * @param	string	path	Path to the log file.
	 * @throws	ApplicationException
	 */
	static public function setLogFile($path) {
		self::$_logPath = $path;
	}
	/** Set to use the standard output. */
	static public function setStdout() {
		self::$_logPath = "php://stdout";
	}
	/** Set to use the standard error output. */
	static public function setStderr() {
		self::$_logPath = "php://stderr";
	}
	/**
	 * Set the priority threshold.
	 * @param	int|array	$threshold	Threshold value (FineLog::DEBUG, FineLog::INFO, ...) or
	 *						a list of thresholds for log classes.
	 */
	static public function setThreshold($threshold) {
		if (is_array($threshold))
			self::$_threshold = $threshold;
		else {
			self::$_threshold = array();
			self::$_threshold[self::DEFAULT_CLASS] = $threshold;
		}
	}
	/**
	 * Write a log message. It the priority level is not defined, it will be INFO.
	 * Use this method only for temporary log messages. For definitive messages, use the fullLog() method.
	 * @param	mixed	$classOrMessageOrPriority	Log message, or priority level, or log class.
	 * @param	mixed	$messageOrPriority		(optional) Log message or priority level.
	 * @param	string	$message			(optional) Log message.
	 */
	static public function log($classOrMessageOrPriority, $messageOrPriority=null, $message=null) {
		if (!is_null($message) && !is_null($messageOrPriority))
			self::_writeLog($classOrMessageOrPriority, $messageOrPriority, $message);
		else if (!is_null($messageOrPriority))
			self::_writeLog(self::DEFAULT_CLASS,  $classOrMessageOrPriority, $messageOrPriority);
		else
			self::_writeLog(self::DEFAULT_CLASS, self::INFO, $classOrMessageOrPriority);
	}
	/**
	 * Write a detailed log message.
	 * The first parameter is optional.
	 * @param	string	$classOrPriority	Log class or priority level.
	 * @param	int	$priorityOrMessage	Priority level or log message.
	 * @param	string	$messageOrFile		Log message, or current file name.
	 * @param	string	$fileOrLine		Current filename or current line number.
	 * @param	int	$lineOrCaller		(optional) Current line number or current method name.
	 * @param	string	$caller			(optional) Current method name.
	 */
	static public function fullLog($classOrPriority, $priorityOrMessage, $messageOrFile, $fileOrLine, $lineOrCaller, $caller=null) {
		if (is_null($caller)) {
			// 5 parameters: no log class
			$caller = $lineOrCaller;
			$line = $fileOrLine;
			$file = $messageOrFile;
			$message = $priorityOrMessage;
			$priority = $classOrPriority;
			$class = self::DEFAULT_CLASS;
		} else {
			// 6 parameters: log class
			$line = $lineOrCaller;
			$file = $fileOrLine;
			$message = $messageOrFile;
			$priority = $priorityOrMessage;
			$class = $classOrPriority;
		}
		$txt = "[" . basename($file) . ":$line]";
		if (!empty($caller))
			$txt .= " $caller()";
		self::_writeLog($class, $priority, "$txt: $message");
	}

	/* ********************** PRIVATE METHODS *************** */
	/**
	 * Write a message in the log file, if its priority reach the threshold.
	 * @param	string	$class		Message's log class.
	 * @param	int	$priority	Message's priority level.
	 * @param	string	$message	Log message.
	 * @throws	IOException
	 */
	static private function _writeLog($class, $priority, $message) {
		// the message is not written if its priority is under the threshold
		if (!isset(self::$_threshold[$class]) || $priority < self::$_threshold[$class])
			return;
		// open the file if needed
		if (!empty(self::$_logPath))
			$path = self::$_logPath;
		else if (!empty($_SERVER["FINE_LOG_PATH"]))
			$path = $_SERVER["FINE_LOG_PATH"];
		else
			throw new ApplicationException("No log file set.", ApplicationException::API);
		$text = date("c") . " " . self::$_labels[$priority] . " ";
		if (!empty($class) && $class != self::DEFAULT_CLASS)
			$text .= "-$class- ";
		$text .= $message . "\n";
		if (file_put_contents($path, $text, (substr($path, 0, 6) != "php://" ? FILE_APPEND : null)) === false)
			throw new IOException("Unable to write on log file '$path'.", IOException::UNWRITABLE);
	}
}

} // class_exists

?>
