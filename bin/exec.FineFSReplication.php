#!/usr/bin/php
<?php

$fineFSroot = dirname(__FILE__) . "/..";

require_once("$fineFSroot/lib/php/utils/except.ApplicationException.php");
require_once("$fineFSroot/lib/php/utils/class.FineLog.php");
require_once("$fineFSroot/lib/php/utils/class.FineLock.php");
require_once("$fineFSroot/lib/php/class.FineFSManager.php");

/**
 * FineFS replication program.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, Fine Media
 * @package	FineFS
 * @subpackage	bin
 * @version	$Id$
 */

// lock and processing
$lock = new FineLock();
try {
	$lock->lock();
	// read configuration
	$conf = FineFSManager::readConfiguration();
	// log definition
	FineLog::setLogFile("$fineFSroot/log/replication.log");
	FineLog::setThreshold(FineLog::DEBUG);
	$thresholds = array(
		 "default"	=> FineLog::WARN,
		 "finefs"	=> eval("return (FineLog::" . $conf['base']['loglevel'] . ");")
	);
	FineLog::setThreshold($thresholds);
	// start replication
	FineLog::fullLog("finefs", FineLog::DEBUG, "Start replication.", __FILE__, __LINE__, __CLASS__);
	$replication = new FineFSReplication($conf);
	FineLog::fullLog("finefs", FineLog::DEBUG, "Stop replication.", __FILE__, __LINE__, __CLASS__);
	$lock->unlock();
} catch (Exception $e) {
	print(date("c") . " Replication error: " . $e->getMessage() . "\n");
}

/** Object for management of replication between nodes. */
class FineFSReplication {
	/** Configuration. */
	private $_conf = null;

	/** Constructor. */
	public function __construct($conf) {
		$this->_conf = $conf;
		// process files to propagate
		$this->_processFiles();
		// process errors
		$this->_processErrors();
	}

	/* **************** PRIVATE METHODS *************** */
	/** Process files to propagate over the cluster. */
	private function _processFiles() {
		global $fineFSroot;

		$files = glob("$fineFSroot/log/filestoprocess/*");
		sort($files);
		// loop on the file-to-process list
		foreach ($files as $fileToProcess) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Process file '$fileToProcess'.", __FILE__, __LINE__, __CLASS__);
			$filename = trim(@file_get_contents($fileToProcess));
			$filepath = $this->_conf['base']['dataRoot'] . '/' . $filename;
			$infopath = $this->_conf['base']['infoRoot'] . '/' . $filename;
			if (is_file($filepath) && is_file($infopath)) {
				$params = @parse_ini_file($infopath);
				// send a PUTDATA request to the next server
				foreach ($this->_conf['addresses']['peers'] as $peer) {
					if (in_array($peer, $this->_conf['addresses']['disabled'])) {
						FineFSManager::addFileToErrorLog("add", $peer, $filename);
						continue;
					}
					try {
						FineFSManager::requestPutDataFromFile($peer, $filename, $filepath, $params, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
						FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success on server '$peer'.", __FILE__, __LINE__, __CLASS__);
						break;
					} catch (Exception $e) {
						FineLog::fullLog("finefs", FineLog::INFO, "Unable to send file '$filename' to server '$peer'.", __FILE__, __LINE__, __CLASS__);
						FineFSManager::addFileToErrorLog("add", $peer, $filename);
					}
				}
			}
			// remove the log file
			unlink($fileToProcess);
		}
	}
	/** Process errors. */
	private function _processErrors() {
		global $fineFSroot;

		$files = glob("$fineFSroot/log/errorstoprocess/*");
		sort($files);
		// loop on the errors-to-process list
		foreach ($files as $errorToProcess) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Process file '$errorToProcess'.", __FILE__, __LINE__, __CLASS__);
			$content = file($errorToProcess, FILE_IGNORE_NEW_LINES |  FILE_SKIP_EMPTY_LINES);
			$server = trim($content[0]);
			$cmd = trim($content[1]);
			$filename = trim($content[2]);
			$newname = trim($content[3]);
			if (in_array($server, $this->_conf['addresses']['disabled']))
				continue;
			$filepath = $this->_conf['base']['dataRoot'] . '/' . $filename;
			$infopath = $this->_conf['base']['infoRoot'] . '/' . $filename;
			$params = @parse_ini_file($infopath);
			if (in_array($server, $this->_conf['addresses']['peers'])) {
				// the destination server is still in the cluster
				try {
					switch ($cmd) {
					case "ADD":
						if (is_file($filepath) && is_file($infopath) && is_array($params)) {
							FineFSManager::requestPutDataFromFile($server, $filename, $filepath, $params, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
							FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success on server '$server'.", __FILE__, __LINE__, __CLASS__);
						}
						break;
					case "DEL":
						FineFSManager::requestDel($server, $filename, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
						FineLog::fullLog("finefs", FineLog::DEBUG, "DEL success on server '$server'.", __FILE__, __LINE__, __CLASS__);
						break;
					case "REN":
						FineFSManager::requestRename($server, $filename, $newname, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
						FineLog::fullLog("finefs", FineLog::DEBUG, "RENAME success on server '$server'.", __FILE__, __LINE__, __CLASS__);
						break;
					case "LNK":
						FineFSManager::requestLink($server, $filename, $newname, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
						FineLog::fullLog("finefs", FineLog::DEBUG, "LINK success on server '$server'.", __FILE__, __LINE__, __CLASS__);
						break;
					default:
						throw new ApplicationException("Unknown command '$cmd'.", ApplicationException::UNKNOWN);
					}
				} catch (Exception $e) {
					FineLog::fullLog("finefs", FineLog::INFO, "Unable to process command '$cmd' on server '$server'.", __FILE__, __LINE__, __CLASS__);
					// error: the file is not deleted, it will be processed again later
					continue;
				}
			} else {
				// the destination server is not in the cluster anymore
				FineLog::fullLog("finefs", FineLog::INFO, "No server '$server' in the cluster.", __FILE__, __LINE__, __CLASS__);
			}
			// delete the file
			unlink($errorToProcess);
		}
	}
}

?>
