#!/usr/bin/php
<?php

/**
 * Daemon that handles requests on a FineFS cluster node.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, FineMedia
 * @package	FineFS
 * @subpackage	bin
 * @version	$Id$
 */

// signals management
function signalHandler($signo) {
	global $daemon;

	if ($signo == SIGCHLD) {
		while (pcntl_waitpid(-1, $status, WNOHANG) > 0)
			;
	} else
		$daemon->stop();
}
pcntl_signal(SIGTERM, "signalHandler");
pcntl_signal(SIGCHLD, "signalHandler");

// set root directory
$fineFSroot = dirname(__FILE__) . "/..";
chdir($fineFSroot);

// includes
require_once("$fineFSroot/lib/php/utils/class.FineLock.php");
require_once("$fineFSroot/lib/php/utils/class.FineLog.php");
require_once("$fineFSroot/lib/php/utils/except.IOException.php");
require_once("$fineFSroot/lib/php/class.FineFSManager.php");

// read configuration
$conf = FineFSManager::readConfiguration();
// set user
$userInfo = posix_getpwnam($conf['base']['user']);
if (($userInfo['gid'] != posix_getgid() && !posix_setgid($userInfo['gid']))) {
	fwrite(STDERR, "Unable to set group. Aborting.\n");
	exit(1);
}
if (($userInfo['uid'] != posix_getuid() && !posix_setuid($userInfo['uid']))) {
	fwrite(STDERR, "Unable to set user. Aborting.\n");
	exit(1);
}
// daemonize
if (pcntl_fork())
	exit(0);
posix_setsid();
umask(0);
ini_set("max_execution_time", "0");
ini_set("max_input_time", "0");
set_time_limit(0);
ob_implicit_flush(true);
// lock
$lock = new FineLock();
try {
	$lock->lock(__FILE__, 0);
} catch (IOException $ioe) {
	fwrite(STDERR, "Unable to lock. Aborting.\n");
	exit(2);
}
// close file descriptors
fclose(STDIN);
fclose(STDOUT);
fclose(STDERR);
// log definition
FineLog::setLogFile("$fineFSroot/log/daemon.log");
FineLog::setThreshold(FineLog::DEBUG);
$thresholds = array(
	"default"	=> FineLog::WARN,
	"finefs"	=> eval("return (FineLog::" . $conf['base']['loglevel'] . ");")
);
FineLog::setThreshold($thresholds);

// creates and starts the daemon
$daemon = new FineFSDaemon($conf);
$daemon->listen();
$daemon->loop();
$lock->unlock();

/** Object managing FineFS client-server communication. */
class FineFSDaemon {
	/** Protocol version. */
	private $_protocolVersion = "0.8";
	/** Configuration. */
	private $_conf = null;
	/** Main socket, used to listen for new connections. */
	private $_mainSock = null;
	/** Socket to the client. */
	private $_clientSock = null;
	/** Loop indicator. */
	private $_mustLoop = true;
	/** Name of the file pointed out by the request. */
	private $_filename = null;
	/** Path to the local binary file. */
	private $_filepath = null;
	/** Path to the local metadata file. */
	private $_infopath = null;
	/** Parameters received in the request. */
	private $_params = null;

	/** Constructor. */
	public function __construct($conf) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Daemon creation.", __FILE__, __LINE__, __CLASS__);
		$this->_conf = $conf;
	}
	/**
	 * Creates a connection and listen for incoming connections.
	 * @throws	IOException	If the socket wasn't created.
	 */
	public function listen() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Listening.", __FILE__, __LINE__, __CLASS__);
		$this->_mainSock = stream_socket_server("tcp://0.0.0.0:" . $this->_conf['base']['port'], $errno, $errstr);
		if ($this->_mainSock === false) {
			FineLog::fullLog("finefs", FineLog::ERROR, "Unable to create a socket listening on port '" . $this->_conf['base']['port'] . "': $errstr.",
					 __FILE__, __LINE__, __CLASS__);
			throw new IOException("Unable to create a socket listening on port '" . $this->_conf['base']['port'] . "': $errstr.", IOException::FUNDAMENTAL);
		}
	}
	/**
	 * Daemon's main loop.
	 * @throws	IOException	If the socket wasn't created.
	 */
	public function loop() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Loop.", __FILE__, __LINE__, __CLASS__);
		while ($this->_mustLoop) {
			if (($this->_clientSock = @stream_socket_accept($this->_mainSock, -1)) === false) {
				// processing was interrupted by a signal
				if (pcntl_waitpid(-1, $status, WNOHANG) > 0) {
					// it should be a SIGCHLD, so we clean up all waiting zombie children
					while (pcntl_waitpid(-1, $status, WNOHANG) > 0)
						;
				} else {
					// it was another signal
					$this->_mustLoop = false;
				}
				continue;
			}
			if (($pid = pcntl_fork()) == -1) {
				fclose($this->_mainSock);
				FineLog::fullLog("finefs", FineLog::ERROR, "Unable to fork on incoming connection.", __FILE__, __LINE__, __CLASS__);
				throw new ApplicationException("Unable to fork on incoming connection.", ApplicationException::SYSTEM);
			} else if ($pid == 0) {
				fclose($this->_mainSock);
				// child processing
				$this->_process();
				exit(0);
			}
			fclose($this->_clientSock);
		}
		// wait for all children
		pcntl_waitpid(-1, $status);
	}
	/** End of process. */
	public function stop() {
		$this->_mustLoop = false;
	}

	/** Main processing function. */
	private function _process() {
		// get the command and file names
		$cmd = "";
		$line = trim(fgets($this->_clientSock));
		if (($pos = strpos($line, " ")) === false ||
		    ($cmd = trim(substr($line, 0, $pos))) === null ||
		    ($this->_filename = trim(substr($line, $pos + 1))) === null ||
		    empty($cmd) || empty($this->_filename)) {
			FineLog::fullLog("finefs", FineLog::ERROR, "Missing parameter.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "PROTOCOL");
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "REQUEST - cmd='$cmd' - file='" . $this->_filename . "'.", __FILE__, __LINE__, __CLASS__);
		// file name checking
		$this->_filename = trim($this->_filename);
		$this->_filename = trim($this->_filename, '/');
		if (strpos($this->_filename, "..") !== false) {
			FineLog::fullLog("finefs", FineLog::ERROR, "Forbidden path.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILENAME");
		}
		// paths creation
		$this->_filepath = $this->_conf['base']['dataRoot'] . '/' . $this->_filename;
		$this->_infopath = $this->_conf['base']['infoRoot'] . '/' . $this->_filename;
		// parameters extraction
		$this->_params = FineFSManager::extractParams($this->_clientSock);

		// commands management
		switch ($cmd) {
		case "HELO":
			$this->_cmdHelo();
			break;
		case "LINK":
			$this->_cmdLink();
			break;
		case "PUTHEAD":
			$this->_cmdPutHead();
			break;
		case "PUTDATA":
			$this->_cmdPutData();
			break;
		case "GETDATA":
			$this->_cmdGetData();
			break;
		case "GETHEAD":
			$this->_cmdGetHead();
			break;
		case "LIST":
			$this->_cmdList();
			break;
		case "DEL":
			$this->_cmdDel();
			break;
		case "RENAME":
			$this->_cmdRename();
			break;
		default:
			// unknown command
			FineLog::fullLog("finefs", FineLog::ERROR, "Unknown command.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "COMMAND");
		}
		exit(0);
	}

	/* ******************** COMMANDS ***************** */
	/** HELO command processing. */
	private function _cmdHelo() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "HELO processing.", __FILE__, __LINE__, __CLASS__);
		$this->_response(true);
		$this->_print("protocol: " . $this->_protocolVersion . "\r\n");
		$this->_print("server: " . $this->_conf['addresses']['local'] . "\r\n");
		if ($this->_filename == $this->_conf['addresses']['local'] ||
		    in_array($this->_filename, $this->_conf['addresses']['peers']))
			$this->_print("client: known\r\n");
		else
			$this->_print("client: unknown\r\n");
		$this->_print("\r\n");
		FineLog::fullLog("finefs", FineLog::DEBUG, "HELO success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** GETHEAD command processing. */
	private function _cmdGetHead() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETHEAD processing.", __FILE__, __LINE__, __CLASS__);
		if (!file_exists($this->_infopath)) {
			FineLog::fullLog("finefs", FineLog::INFO, "No metadata file.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILENAME");
		}
		$info = @parse_ini_file($this->_infopath);
		$this->_response(true);
		foreach ($info as $key => $val)
			$this->_print("$key: $val\r\n");
		$this->_print("\r\n");
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETHEAD success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** GETDATA command processing. */
	private function _cmdGetData() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETDATA processing.", __FILE__, __LINE__, __CLASS__);
		// read meta file
		if (!file_exists($this->_infopath) || ($info = @parse_ini_file($this->_infopath)) === false) {
			FineLog::fullLog("finefs", FineLog::INFO, "No metadata file.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILENAME");
		}
		// read binary file
		if (!file_exists($this->_filepath)) {
			// the file is not available locally, we fetch it - check if it's a symlink or a real file
			$fileToFetch = $this->_filename;
			$whereToFetch = $this->_filepath;
			if (is_link($this->_filepath)) {
				// it's a link, we'll fetch its destination
				if (($dest = readlink($this->_filepath)) === false ||
				    substr($dest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot']) {
					FineLog::fullLog("finefs", FineLog::WARN, "The link '" . $this->_filename . "' is pointing to a bad file ($dest).", __FILE__, __LINE__, __METHOD__);
					throw new IOException("The link '" . $this->_filename . "' is pointing to a bad file ($dest).", IOException::NOT_FOUND);
				}
				$fileToFetch = substr($dest, strlen($this->_conf['base']['dataRoot']));
				$whereToFetch = $dest;
			}
			FineFSManager::mkpath(dirname($whereToFetch));
			FineLog::fullLog("finefs", FineLog::DEBUG, "Fetch file from its origin.", __FILE__, __LINE__, __CLASS__);
			if ($this->_params['nofollowup'] == 1 || !$this->_fetchFileFromOrigin($info['origin'], $fileToFetch, $whereToFetch)) {
				FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to get file from its origin.", __FILE__, __LINE__, __CLASS__);
				$this->_response(false, "UNAVAILABLE");
			}
		}
		// open the file
		if (($src = fopen($this->_filepath, "r")) === false) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to open file '" . $this->_filepath . "'.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "UNAVAILABLE");
		}
		// sending data
		$this->_response(true);
		foreach ($info as $key => $val)
			$this->_print("$key: $val\r\n");
		$this->_print("\r\n");
		stream_copy_to_stream($src, $this->_clientSock);
		fclose($src);
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETDATA success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** LIST command processing. */
	private function _cmdList() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "LIST processing.", __FILE__, __LINE__, __CLASS__);
		$files = glob($this->_infopath);
		$this->_response(true);
		$filelist = array();
		$dirlist = array();
		$prefixLen = strlen($this->_conf['base']['dataRoot'] . '/');
		foreach ($files as $file) {
			$filename = substr($file, $prefixLen);
			if (is_dir($file))
				$dirlist[] = $filename;
			else {
				$info = @parse_ini_file($file);
				$filelist[$filename] = $info;
			}
		}
		$this->_print("count: " . (count($dirlist) + count($filelist)) . "\r\n");
		$this->_print("\r\n");
		sort($dirlist);
		ksort($filelist);
		foreach ($dirlist as $filename) {
			$this->_print($filename . "\r\n");
			$this->_print("mime: inode/directory\r\n");
			$this->_print("\r\n");
		}
		foreach ($filelist as $filename => $info) {
			$this->_print($filename . "\r\n");
			foreach ($info as $key => $val)
				$this->_print("$key: $val\r\n");
			$this->_print("\r\n");
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "LIST success", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** DEL command processing. */
	private function _cmdDel() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "DEL processing.", __FILE__, __LINE__, __CLASS__);
		$this->_response(true);
		$this->_closeConnection();
		@unlink($this->_filepath);
		@unlink($this->_infopath);
		// remove empty directories
		$excludeDirs = array($this->_conf['base']['dataRoot'], $this->_conf['base']['infoRoot']);
		FineFSManager::rmEmptyDir(dirname($this->_filepath), $excludeDirs);
		FineFSManager::rmEmptyDir(dirname($this->_infopath), $excludeDirs);
		// stop processing if the DEL request was originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local origin. DEL success.", __FILE__, __LINE__, __CLASS__);
			exit(0);
		}
		// forward to the next server
		$this->_params['origin'] = empty($this->_params['origin']) ? $this->_conf['addresses']['local'] : $this->_params['origin'];
		FineLog::fullLog("finefs", FineLog::DEBUG, "Send request to the next server.", __FILE__, __LINE__, __CLASS__);
		$this->_sendDelToNextPeer();
		FineLog::fullLog("finefs", FineLog::DEBUG, "DEL success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** RENAME command processing. */
	private function _cmdRename() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "RENAME processing.", __FILE__, __LINE__, __CLASS__);
		// checking parameters
		if (empty($this->_params['newfile'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Missing parameter.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "PROTOCOL");
		}
		// stop processing if the RENAME request was originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local origin. RENAME success.", __FILE__, __LINE__, __CLASS__);
			$this->_response(true);
			exit(0);
		}
		// get metadata
		$infos = @parse_ini_file($this->_infopath);
		if ($infos === false || empty($infos)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Missing metadata file (" . $this->_infopath . ").", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILENAME");
		}
		// checking file's identifier
		if (!empty($this->_params['id']) && $this->_params['id'] != $infos['id']) {
			// bad identifier - it's not an error, it could be normal when a server is back online
			FineLog::fullLog("finefs", FineLog::INFO, "Bad identifier '" . $this->_params['id'] . "'.", __FILE__, __LINE__, __CLASS__);
			$this->_response(true);
			exit(0);
		}
		// checking destination name
		$this->_params['newfile'] = trim($this->_params['newfile']);
		$this->_params['newfile'] = trim($this->_params['newfile'], '/');
		if (strpos($this->_params['newfile'], "..") !== false) {
			FineLog::fullLog("finefs", FineLog::ERROR, "Forbidden path '" . $this->_params['newfile'] . "'.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "DESTINATION");
		}
		// checking that the binary file is locally available and is not a symlink
		if (!file_exists($this->_filepath) && !is_link($this->_filepath)) {
			// not available, we fetch it
			FineFSManager::mkpath(dirname($this->_filepath));
			$fetchOrigin = $info['origin'];
			$fetchFilename = $this->_filename;
			if (!empty($this->_params['origin'])) {
				// if the RENAME request has an origin, we use it
				$fetchOrigin = $this->_params['origin'];
				$fetchFilename = $this->_params['newfile'];
			}
			if (!$this->_fetchFileUpToOrigin($fetchOrigin, $fetchFilename, $this->_filepath, false)) {
				FineLog::fullLog("finefs", FineLog::ERROR, "Unable to get file '$fetchFilename' from any server up to its origin '$fetchOrigin'.", __FILE__, __LINE__, __CLASS__);
				$this->_response(false, "FILEPATH");
			}
		}
		// new paths creation
		$newfilepath = $this->_conf['base']['dataRoot'] . '/' . $this->_params['newfile'];
		$newinfopath = $this->_conf['base']['infoRoot'] . '/' . $this->_params['newfile'];
		FineFSManager::mkpath(dirname($newfilepath));
		FineFSManager::mkpath(dirname($newinfopath));
		// moving files
		if (!@rename($this->_infopath, $newinfopath)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to move metadata file.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "INFOPATH");
		}
		if (!@rename($this->_filepath, $newfilepath)) {
			@rename($newinfopath, $this->_infopath);
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to move binary file.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILEPATH");
		}
		$this->_response(true);
		$this->_closeConnection();
		// delete empty directories
		$excludeDirs = array($this->_conf['base']['dataRoot'], $this->_conf['base']['infoRoot']);
		FineFSManager::rmEmptyDir(dirname($filepath), $excludeDirs);
		FineFSManager::rmEmptyDir(dirname($infopath), $excludeDirs);
		// forward to the next server
		$this->_params['origin'] = empty($this->_params['origin']) ? $this->_conf['addresses']['local'] : $this->_params['origin'];
		$this->_params['id'] = $infos['id'];
		FineLog::fullLog("finefs", FineLog::DEBUG, "Send request to the next server.", __FILE__, __LINE__, __CLASS__);
		$this->_sendRenameToNextPeer();
		FineLog::fullLog("finefs", FineLog::DEBUG, "RENAME success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** PUTHEAD command processing. */
	private function _cmdPutHead() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "PUTHEAD processing.", __FILE__, __LINE__, __CLASS__);
		// checking parameters
		if (empty($this->_params['origin']) || empty($this->_params['id']) || empty($this->_params['md5']) ||
		    empty($this->_params['size']) || !is_numeric($this->_params['size'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Missing parameter.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "PROTOCOL");
		}
		// close connection
		$this->_response(true);
		$this->_closeConnection();
		// stop processing if the file were originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local origin. PUTHEAD success.", __FILE__, __LINE__, __CLASS__);
			exit(0);
		}
		// writing metadata to temporary file
		$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], time());
		if (file_put_contents($tmpInfo, FineFSManager::encodeIniFile($this->_params)) === false) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to write temporary metadata file.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "INFOPATH");
		}
		// moving metadata file to its final destination(determining the destination first, depending if it's a link or a real file)
		$infodest = $this->_infopath;
		if (is_link($this->_infopath) &&
		    (($infodest = readlink($this->_infopath)) === false ||
		     substr($infodest, 0, strlen($this->_conf['base']['infoRoot'])) != $this->_conf['base']['infoRoot'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to get information about the link '" . $this->_infopath . "'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			$this->_response(false, "INFOPATH");
		}
		FineFSManager::mkpath(dirname($infodest));
		if (!@rename($tmpInfo, $infodest)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to write metadata file.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			$this->_response(false, "INFOPATH");
		}
		// remove local (binary) file if it exists (determining which file to remove, if it's a link)
		$filedest = $this->_filepath;
		if (is_link($this->_filepath) &&
		    (($filedest = readlink($this->_filepath)) === false ||
		     substr($filedest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to get information about the link '" . $this->_filepath . "'.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "FILEPATH");
		}
		@unlink($filedest);
		// create an empty file if needed, to avoid a second copy of empty binary file
		if ($this->_params['size'] == 0)
			@touch($this->_filepath);
		// forward to the next server
		FineLog::fullLog("finefs", FineLog::DEBUG, "Send request to next server.", __FILE__, __LINE__, __CLASS__);
		$this->_sendPutHeadToNextPeer();
		FineLog::fullLog("finefs", FineLog::DEBUG, "PUTHEAD success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** LINK command processing. */
	private function _cmdLink() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "LINK processing.", __FILE__, __LINE__, __CLASS__);
		// checking parameters
		if (empty($this->_params['target'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Missing parameter 'target'.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "PROTOCOL");
		}
		// stop processing if the file were originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local origin. LINK success.", __FILE__, __LINE__, __CLASS__);
			$this->_response(true);
			exit(0);
		}
		// create paths
		$infoTarget = $this->_conf['base']['infoRoot'] . "/" . $this->_params['target'];
		$fileTarget = $this->_conf['base']['dataRoot'] . "/" . $this->_params['target'];
		// temporary files creation
		$tmpFile = tempnam($this->_conf['base']['tmpRoot'], time());
		$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], time());
		// create links
		@unlink($tmpInfo);
		if (!@symlink($infoTarget, $tmpInfo)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to create link '$tmpInfo' -> '$infoTarget'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			$this->_response(false, "INFOPATH");
		}
		@unlink($tmpFile);
		if (!@symlink($fileTarget, $tmpFile)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to create link '$tmpFile' -> '$fileTarget'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			@unlink($tmpFile);
			$this->_response(false, "INFOPATH");
		}
		// moving temporary files to their final destinations
		FineFSManager::mkpath(dirname($this->_filepath));
		FineFSManager::mkpath(dirname($this->_infopath));
		if (!@rename($tmpInfo, $this->_infopath) || !@rename($tmpFile, $this->_filepath)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary files.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			@unlink($tmpFile);
			@unlink($infopath);
			$this->_response(false, "INFOPATH");
			throw new IOException("Unable to move temporary files.", IOException::UNWRITABLE);
		}
		$this->_response(true);
		$this->_closeConnection();
		// forward to the next server
		$this->_params['origin'] = empty($this->_params['origin']) ? $this->_conf['addresses']['local'] : $this->_params['origin'];
		$this->_sendLinkToNextPeer();
		FineLog::fullLog("finefs", FineLog::DEBUG, "LINK success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}
	/** PUTDATA command processing. */
	private function _cmdPutData() {
		FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA processing.", __FILE__, __LINE__, __CLASS__);
		// checking parameters
		if (empty($this->_params['size']) || !is_numeric($this->_params['size']) || empty($this->_params['md5'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Missing parameter.", __FILE__, __LINE__, __CLASS__);
			$this->_response(false, "PROTOCOL");
		}
		// stop processing if the file were originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local origin. PUTDATA success.", __FILE__, __LINE__, __CLASS__);
			$this->_response(true);
			exit(0);
		}
		// get data only for non-empty files
		if ($this->_params['size'] > 0) {
			// asking for data
			$this->_print("GO\r\n");
			// open the temporary destination file
			$tmpFile = tempnam($this->_conf['base']['tmpRoot'], time());
			if (($fOut = fopen($tmpFile, "w")) === false) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to create file '$tmpFile'.", __FILE__, __LINE__, __CLASS__);
				$this->_response(false, "FILEPATH");
			}
			// read data and write to disk
			$readSize = 0;
			stream_set_blocking($this->_clientSock, 0);
			$selecWrite = null;
			$selectExcept = null;
			while ($readSize < $this->_params['size'] && !feof($this->_clientSock)) {
				set_time_limit(0);
				$selectRead = array($this->_clientSock);
				if (@stream_select($selectRead, $selectWrite, $selectExcept, 2)) {
					if (($chunk = fread($this->_clientSock, 8192)) === false)
						break;
					fwrite($fOut, $chunk);
					$readSize += mb_strlen($chunk, 'ASCII');
				}
			}
			fclose($fOut);
			// check data size
			if ($readSize != $this->_params['size']) {
				FineLog::fullLog("finefs", FineLog::INFO, "Error on file size.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpFile);
				$this->_response(false, "SIZE");
			}
			// check MD5
			$writtenMD5 = md5_file($tmpFile);
			if ($writtenMD5 != $this->_params['md5']) {
				FineLog::fullLog("finefs", FineLog::INFO, "Error on MD5sum.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpFile);
				$this->_response(false, "MD5");
			}
		}
		// write temporary metadata file
		$timestamp = time();
		$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], $timestamp);
		$this->_params['origin'] = empty($this->_params['origin']) ? $this->_conf['addresses']['local'] : $this->_params['origin'];
		$this->_params['id'] = empty($this->_params['id']) ? base_convert(md5($timestamp . $writtenMD5), 16, 36) : $this->_params['id'];
		$this->_params['date'] = empty($this->_params['date']) ? date("c", $timestamp) : $this->_params['date'];
		if (file_put_contents($tmpInfo, FineFSManager::encodeIniFile($this->_params)) === false) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to write metadata file.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpFile);
			$this->_response(false, "INFOPATH");
		}
		// handling symlinks
		$infodest = $this->_infopath;
		if (is_link($this->_infopath) &&
		    (($infodest = readlink($this->_infopath)) === false ||
		     substr($infodest, 0, strlen($this->_conf['base']['infoRoot'])) != $this->_conf['base']['infoRoot'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to get information about the link '" . $this->_infopath . "'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			@unlink($tmpFile);
			$this->_reponse(false, "INFOPATH");
		}
		$filedest = $this->_filepath;
		if (is_link($this->_filepath) &&
		    (($filedest = readlink($this->_filepath)) === false ||
		     substr($filedest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot'])) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to get information about the link '" . $this->_filepath . "'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			@unlink($tmpFile);
			$this->_response(false, "FILEPATH");
		}
		// moving temporary files to their final destinations
		FineFSManager::mkpath(dirname($filedest));
		FineFSManager::mkpath(dirname($infodest));
		if (!@rename($tmpInfo, $infodest)) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary metadata file '$tmpInfo' to '$infodest'.", __FILE__, __LINE__, __CLASS__);
			@unlink($tmpInfo);
			@unlink($tmpFile);
			$this->_response(false, "INFOPATH");
		}
		if ($this->_params['size'] > 0) {
			// non-empty file, move it
			if (!@rename($tmpFile, $filedest)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary file '$tmpFile' to '$filedest'.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpFile);
				$this->_response(false, "FILEPATH");
			}
		} else {
			// empty file, create it directly
			if (!@unlink($filedest) || !@touch($filedest)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to create empty file '$filedest'.", __FILE__, __LINE__, __CLASS__);
				$this->_response(false, "FILEPATH");
			}
		}
		$this->_response(true);
		$this->_closeConnection();
		if ($this->_params['size'] > 0) {
			// non-empty file - add to the "new files" log
			FineLog::fullLog("finefs", FineLog::DEBUG, "Add to 'new files' log.", __FILE__, __LINE__, __CLASS__);
			FineFSManager::addFileToProcessLog($this->_filename);
		}
		// send a PUTHEAD request to the next server, if the file is originated from this server
		if ($this->_params['origin'] == $this->_conf['addresses']['local']) {
			FineLog::fullLog("finefs", FineLog::DEBUG, "Send PUTHEAD request to next server.", __FILE__, __LINE__, __CLASS__);
			$this->_sendPutHeadToNextPeer();
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success.", __FILE__, __LINE__, __CLASS__);
		exit(0);
	}

	/* ******************** PRIVATE METHODS ***************** */
	/**
	 * Fetch a file from its origin server, or another server.
	 * @param	string	$origin		Name of the origin server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$filepath	Local path of writing.
	 * @param	bool	$transfer	(optional) Send the file to next server, or not. True by default.
	 * @return	bool	True if the file was correctly fetched, False otherwise.
	 */
	private function _fetchFileFromOrigin($origin, $filename, $filepath, $transfer=true) {
		if (!is_array($this->_conf['addresses']['peers'])) {
			FineLog::fullLog("finefs", FineLog::WARN, "No usable remote server.", __FILE__, __LINE__, __METHOD__);
			return (false);
		}
		// creating temporary file name
		$tmpPath = tempnam($this->_conf['base']['tmpRoot'], time());
		// fetching the file
		$serverFound = false;
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if ($peer == $origin)
				$serverFound = true;
			if (in_array($peer, $this->_conf['addresses']['disabled']))
				continue;
			if ($serverFound) {
				try {
					// get the file and write it on the temporary destination
					FineFSManager::requestGetFileFromData($peer, $filename, $tmpPath, false, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					// move the file to its final destination
					if (!@rename($tmpPath, $filepath)) {
						FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename '$tmpPath' to '$filepath'.", __FILE__, __LINE__, __CLASS__);
						@unlink($tmpPath);
						throw new IOException("", IOException::UNWRITABLE);
					}
					if ($transfer)
						FineFSManager::addFileToProcessLog($filename);
					return (true);
				} catch (Exception $e) {
					FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to fetch file '$filename' from server '$peer'.", __FILE__, __LINE__, __CLASS__);
				}
			}
		}
		return (false);
	}
	/**
	 * Fetch a file from the previous server, up to its origin.
	 * @param	string	$origin		Name of the origin server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$filepath	Local path of writing.
	 * @param	bool	$transfer	(optional) Send the file to the next server, or not. False by default.
	 * @return	bool	True if the file was correctly fetched, False otherwise.
	 */
	private function _fetchFileUpToOrigin($origin, $filename, $filepath, $transfer=false) {
		if (!is_array($this->_conf['addresses']['peers'])) {
			FineLog::fullLog("finefs", FineLog::WARN, "No usable remote server.", __FILE__, __LINE__, __METHOD__);
			return (false);
		}
		// creating temporary file name
		$tmpPath = tempnam($this->_conf['base']['tmpRoot'], time());
		// fetching file
		$servers = array_reverse($this->_conf['addresses']['peers']);
		foreach ($servers as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				if ($peer == $origin)
					break;
				continue;
			}
			try {
				// get the file and write it on the temporary destination
				FineFSManager::requestGetFileFromData($peer, $filename, $tmpPath, false, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				// move the file to its final destination
				if (!@rename($tmpPath, $filepath)) {
					FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename '$tmpPath' to '$filepath'.", __FILE__, __LINE__, __CLASS__);
					@unlink($tmpPath);
					throw new IOException("", IOException::UNWRITABLE);
				}
				if ($transfer)
					FineFSManager::addFileToProcessLog($filename);
				return (true);
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to fetch file '$filename' from server '$peer'.", __FILE__, __LINE__, __CLASS__);
			}
			if ($peer == $origin)
				break;
		}
		return (false);
	}
	/** Send a LINK request to the next server. */
	private function _sendLinkToNextPeer() {
		if (!is_array($this->_conf['addresses']['peers']))
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("lnk", $peer, $this->_filename, $this->_params['target']);
				continue;
			}
			try {
				FineFSManager::requestLink($peer, $this->_filename, $this->_params['target'], $this->_params['origin'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				FineLog::fullLog("finefs", FineLog::DEBUG, "LINK success on server '$peer'.", __FILE__, __LINE__, __CLASS__);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send LINK request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("lnk", $peer, $this->_filename, $this->_params['target']);
			}
		}
	}
	/** Send a PUTHEAD request to the next server. */
	private function _sendPutHeadToNextPeer() {
		if (!is_array($this->_conf['addresses']['peers']))
			return;
		// send the metadata file to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			try {
				FineFSManager::requestPutHead($peer, $this->_filename, $this->_params, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				FineLog::fullLog("finefs", FineLog::DEBUG, "PUTHEAD success on server '$peer'.", __FILE__, __LINE__, __CLASS__);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send PUTHEAD to server '$peer'.", __FILE__, __LINE__, __CLASS__);
			}
		}
	}
	/** Send a DEL request to the next server. */
	private function _sendDelToNextPeer() {
		if (!is_array($this->_conf['addresses']['peers']))
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("del", $peer, $this->_filename);
				continue;
			}
			try {
				FineFSManager::requestDel($peer, $this->_filename, $this->_params['origin'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				FineLog::fullLog("finefs", FineLog::DEBUG, "DEL success on server '$peer'.", __FILE__, __LINE__, __CLASS__);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send DEL request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("del", $peer, $this->_filename);
			}
		}
	}
	/** Send a RENAME request to the next server. */
	private function _sendRenameToNextPeer() {
		if (!is_array($this->_conf['addresses']['peers']))
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("ren", $peer, $this->_filename, $this->_params['newfile']);
				continue;
			}
			try {
				FineFSManager::requestRename($peer, $this->_filename, $this->_params['newfile'], $this->_params['origin'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				FineLog::fullLog("finefs", FineLog::DEBUG, "RENAME success on server '$peer'.", __FILE__, __LINE__, __CLASS__);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send RENAME request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("ren", $peer, $this->_filename, $this->_params['newfile']);
			}
		}
	}
	/**
	 * Send the response to the server. If it's a failure, the program is stopped.
	 * @param	bool	$success	True for a success response, False for a failure.
	 * @param	string	$special	(optional) Special response (error type or particular success).
	 */
	private function _response($success, $special="") {
		if ($success === false) {
			$this->_print("ERR $special\r\n");
			$this->_closeConnection();
			exit(0);
		}
		$this->_print("OK" . (empty($special) ? "" : " $special") . "\r\n");
	}
	/** Write a string to the client socket. */
	private function _print($string) {
		fwrite($this->_clientSock, $string);
	}
	/** Close client connection. */
	private function _closeConnection() {
		fclose($this->_clientSock);
	}
}

?>
