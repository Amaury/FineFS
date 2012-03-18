<?php

require_once(dirname(__FILE__) . "/utils/class.FineLog.php");
require_once(dirname(__FILE__) . "/utils/except.ApplicationException.php");
require_once(dirname(__FILE__) . "/class.FineFSManager.php");

/**
 * General FineFS management.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, FineMedia
 * @package	FineFS
 * @subpackage	lib
 * @version	$Id$
 */
class FineFS {
	/** Unique instance. */
	static private $_instance = null;
	/** Configuration. */
	private $_conf = null;
	/** Direct access to local data or not. */
	private $_useLocalData = false;
	/** Can connect to local daemon or not. */
	private $_useLocalDaemon = false;
	/** Can connect to remote server or not. */
	private $_useDistantPeer = false;

	/* **************** CONSTRUCTION METHODS ******************* */
	/**
	 * Singleton. Returns a unique instance of FineFS.
	 * @param	string	$conf	(optional) Path to the configuration file, or textual content of the configuration (INI format).
	 *				By default, the configuration is read from file 'etc/finefs.ini'.
	 * @param	bool	$sync	(optional) Set to TRUE if the communication should be sychron whenever possible. TRUE by default.
	 * @return	FineFS	A FineFS object.
	 */
	static public function singleton($conf=null, $sync=true) {
		if (!isset(self::$_instance))
			self::$_instance = FineFS::factory($conf, $sync);
		return (self::$_instance);
	}
	/**
	 * Factory. Use this method to create a FineFS object.
	 * @param	string	$conf	(optional) Path to the configuration file, or textual content of the configuration (INI format).
	 *				By default, the configuration is read from file 'etc/finefs.ini'.
	 * @param	bool	$sync	(optional) Set to TRUE if the communication should be sychron whenever possible. TRUE by default.
	 * @return	FineFS	A new FineFS object.
	 */
	static public function factory($conf=null, $sync=true) {
		return (new FineFS($conf, $sync));
	}
	/**
	 * Constructor.
	 * @param	string	$conf	(optional) Path to the configuration file, or textual content of the configuration (INI format).
	 *				By default, the configuration is read from file 'etc/finefs.ini'.
	 * @param	bool	$sync	(optional) Set to TRUE if the communication should be sychron whenever possible. TRUE by default.
	 */
	private function __construct($conf=null, $sync=true) {
		// read the configuration
		$this->_conf = FineFSManager::readConfiguration($conf);
		// set the sync/async behaviour
		$this->setSyncBehaviour($sync);
		FineLog::fullLog("finefs", FineLog::DEBUG, "FineFS object created.", __FILE__, __LINE__, __METHOD__);
	}
	/**
	 * Define the synchron/asynchron behaviour of the object.
	 * File readings and writings are faster using synchron communication (direct disk access), but file writings involve
	 * network communication overhead.
	 * @param	bool	$sync	(optional) Set to TRUE if the communication should be sychron whenever possible. TRUE by default.
	 * @return	bool	TRUE if file operations will be synchronized.
	 */
	public function setSyncBehaviour($sync=true) {
		// how data are accessible?
		if (!empty($this->_conf['addresses']['local']) && !empty($this->_conf['base']['user'])) {
			// can use local daemon
			$this->_useLocalDaemon = true;
			$scriptUser = posix_getpwuid(posix_geteuid());
			if ($sync && $this->_conf['base']['user'] == $scriptUser['name']) {
				// can access directly to local data on disk
				$this->_useLocalData = true;
			}
		}
		if (($n = count($this->_conf['addresses']['peers'])) > 0 &&
		    count($this->_conf['addresses']['disabled']) < $n) {
			// can connect to a remote server
			$this->_useDistantPeer = true;
		}
		return ($this->_useLocalData);
	}

	/* *********************** PROCESSING METHODS ******************* */
	/**
	 * Ask for server information.
	 * @return	array	Hash with keys 'protocol', 'server' and 'client'.
	 * @throws	ApplicationException	If it's not possible to connect to a server (no configured server or no answering server).
	 * @throws	IOException
	 */
	public function helo() {
		if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$data = FineFSManager::requestHelo("localhost", $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Helo success.", __FILE__, __LINE__, __METHOD__);
			return ($data);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					$data = FineFSManager::requestHelo($server[0], $this->_conf['addresses']['local'], $this->_conf['base']['port'],
									   $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Helo success.", __FILE__, __LINE__, __METHOD__);
					return ($data);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Create a symlink on the cluster.
	 * @param	string	$filename	Name of the file to create.
	 * @param	string	$target		Path of the target on the cluster.
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function link($filename, $target) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Create link '$filename' -> '$target'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			$infoTarget = $this->_conf['base']['infoRoot'] . "/" . $target;
			$fileTarget = $this->_conf['base']['dataRoot'] . "/" . $target;
			// temporary files creation
			$tmpFile = tempnam($this->_conf['base']['tmpRoot'], time());
			$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], time());
			// create links
			@unlink($tmpInfo);
			if (!@symlink($infoTarget, $tmpInfo)) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to create link '$tmpInfo' -> '$infoTarget'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to create link.", IOException::UNWRITABLE);
			}
			@unlink($tmpFile);
			if (!@symlink($fileTarget, $tmpFile)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to create link '$tmpFile' -> '$fileTarget'.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpInfo);
				throw new IOException("Unable to create link.", IOException::UNWRITABLE);
			}
			// moving temporary files to their final destinations
			FineFSManager::mkpath(dirname($filepath));
			FineFSManager::mkpath(dirname($infopath));
			if (!rename($tmpInfo, $infopath) || !rename($tmpFile, $filepath)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary files.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpInfo);
				@unlink($tmpFile);
				@unlink($infopath);
				throw new IOException("Unable to move temporary files.", IOException::UNWRITABLE);
			}
			// send a LINK request to the next server
			$this->_sendLinkToNextPeer($filename, $target);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Create link success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			FineFSManager::requestLink("localhost", $filename, $target, null, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Create link success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {   
					FineFSManager::requestLink($server[0], $filename, $target, null, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Create link success.", __FILE__, __LINE__, __METHOD__);
					return;
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Store a data stream on the cluster.
	 * @param	string	$filename	Name of the file to create.
	 * @param	binary	$data		Content of the file.
	 * @param	array	$info		(optional) Hash containing file's metadata.
	 * @throws	ApplicationException	If it's not possible to connect to a server (no configured server or no answering server).
	 * @throws	IOException
	 */
	public function putData($filename, $data, $info=null) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Put data for file '$filename'.", __FILE__, __LINE__, __METHOD__);
		unset($info['origin']);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			$size = mb_strlen($data, "ASCII");
			$md5 = md5($data);
			$info = is_array($info) ? $info : array();
			$timestamp = time();
			$info['origin'] = $this->_conf['addresses']['local'];
			$info['id'] = base_convert(md5($timestamp . $md5), 16, 36);
			$info['date'] = date("c", $timestamp);
			$info['size'] = $size;
			$info['md5'] = $md5;
			// temporary files creation
			$tmpFile = tempnam($this->_conf['base']['tmpRoot'], time());
			$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], time());
			// metadata file writing
			if (file_put_contents($tmpInfo, FineFSManager::encodeIniFile($info)) === false) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to write file '$tmpInfo'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to write file '$tmpInfo'.", IOException::UNWRITABLE);
			}
			// file writing
			if (file_put_contents($tmpFile, $data) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to write file '$tmpFile'.", __FILE__, __LINE__, __METHOD__);
				@unlink($tmpInfo);
				throw new IOException("Unable to write file '$tmpFile'.", IOException::UNWRITABLE);
			}
			// moving temporary files to their final destinations (determining the destination first, depending if it's a link or a real file)
			$filedest = $filepath;
			if (is_link($filepath) && ($filedest = readlink($filepath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to get information about the link '$filepath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to get information about the link '$filepath'.", IOException::UNREADABLE);
			}
			$infodest = $infopath;
			if (is_link($infopath) && ($infodest = readlink($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to get information about the link '$infopath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to get information about the link '$infopath'.", IOException::UNREADABLE);
			}
			FineFSManager::mkpath(dirname($filedest));
			FineFSManager::mkpath(dirname($infodest));
			if (!rename($tmpInfo, $infodest) || !rename($tmpFile, $filedest)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary files.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpInfo);
				@unlink($tmpFile);
				throw new IOException("Unable to move temporary files.", IOException::UNWRITABLE);
			}
			if ($size > 0) {
				// non-empty file - add to the "new files" log
				FineFSManager::addFileToProcessLog($filename);
			}
			// send a PUTHEAD request to the next server
			$this->_sendPutHeadToNextPeer($filename, $info);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Put data success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			FineFSManager::requestPutData("localhost", $filename, $data, $info, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Put data success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					FineFSManager::requestPutData($server[0], $filename, $data, $info, $this->_conf['base']['port'],
								      $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Put data success.", __FILE__, __LINE__, __METHOD__);
					return;
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Store the content of a local file on the cluster.
	 * @param	string	$filename	Name of the file to create.
	 * @param	string	$localpath	Path to the local file.
	 * @param	array	$info		(optional) Hash containing file's metadata.
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function putFile($filename, $localpath, $info=null) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Put file '$filename'.", __FILE__, __LINE__, __METHOD__);
		unset($info['origin']);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			if (!is_readable($localpath)) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to read file '$localpath'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("Unable to read file '$localpath'.", IOException::UNREADABLE);
			}
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			$size = filesize($localpath);
			$md5 = md5_file($localpath);
			$timestamp = time();
			$info = is_array($info) ? $info : array();
			$info['origin'] = $this->_conf['addresses']['local'];
			$info['id'] = base_convert(md5($timestamp . $md5), 16, 36);
			$info['date'] = date("c", $timestamp);
			$info['size'] = $size;
			$info['md5'] = $md5;
			// temporary files creation
			$tmpFile = tempnam($this->_conf['base']['tmpRoot'], time());
			$tmpInfo = tempnam($this->_conf['base']['tmpRoot'], time());
			// file copy
			if (!@copy($localpath, $tmpFile)) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to copy file '$localpath' to '$tmpFile'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("Unable to copy file '$localpath' to '$tmpFile'.", IOException::UNWRITABLE);
			}
			// metadata file writing
			if (file_put_contents($tmpInfo, FineFSManager::encodeIniFile($info)) === false) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to write file '$tmpInfo'.", __FILE__, __LINE__, __CLASS__);
				@unlink($filepath);
				throw new IOException("Unable to write file '$tmpInfo'.", IOException::UNWRITABLE);
			}
			// moving temporary files to their final destinations (determining the destination first, depending if it's a link or a real file)
			$filedest = $filepath;
			if (is_link($filepath) && ($filedest = readlink($filepath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to get information about the link '$filepath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to get information about the link '$filepath'.", IOException::UNREADABLE);
			}
			$infodest = $infopath;
			if (is_link($infopath) && ($infodest = readlink($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to get information about the link '$infopath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to get information about the link '$infopath'.", IOException::UNREADABLE);
			}
			FineFSManager::mkpath(dirname($filedest));
			FineFSManager::mkpath(dirname($infodest));
			if (!rename($tmpInfo, $infodest) || !rename($tmpFile, $filedest)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to move temporary files.", __FILE__, __LINE__, __CLASS__);
				@unlink($tmpInfo);
				@unlink($tmpFile);
				throw new IOException("Unable to move temporary files.", IOException::UNWRITABLE);
			}
			if ($size > 0) {
				// non-empty file - add to "new files" log
				FineFSManager::addFileToProcessLog($filename);
			}
			// send a PUTHEAD request to the next server
			$this->_sendPutHeadToNextPeer($filename, $info);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Put file success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			FineFSManager::requestPutDataFromFile("localhost", $filename, $localpath, $info, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Put file success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					FineFSManager::requestPutDataFromFile($server[0], $filename, $localpath, $info, $this->_conf['base']['port'],
									      $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Put file success.", __FILE__, __LINE__, __METHOD__);
					return;
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Returns a list of files matching a search path.
	 * @param	string	$path	Search path (with stars).
	 * @return	array	Hashes list.
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function getList($path) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Get list for path '$path'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$files = glob($this->_conf['base']['infoRoot'] . "/" . $path);
			$filelist = array();
			$dirlist = array();
			$prefixLen = strlen($this->_conf['base']['infoRoot'] . '/');
			foreach ($files as $file) {
				$filename = substr($file, $prefixLen);
				if (is_dir($file))
					$dirlist[$filename] = array( "mime" => "inode/directory" );
				else
					$filelist[$filename] = @parse_ini_file($file);
			}
			ksort($dirlist);
			ksort($filelist);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get list success.", __FILE__, __LINE__, __METHOD__);
			return (array_merge($dirlist, $filelist));
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$list = FineFSManager::requestList("localhost", $path, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get list success.", __FILE__, __LINE__, __METHOD__);
			return ($list);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					$list = FineFSManager::requestList($server[0], $path, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Get list success.", __FILE__, __LINE__, __METHOD__);
					return ($list);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Returns all information about a file (metadata + size + md5).
	 * @param	string	$filename	Name of the file.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function getInfo($filename) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Get info for file '$filename'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			if (($info = @parse_ini_file($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "File not found: '$filename'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("File not found: '$filename'.", IOException::NOT_FOUND);
			}
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get info success.", __FILE__, __LINE__, __METHOD__);
			return ($info);
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$info = FineFSManager::requestGetHead("localhost", $filename, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get info success.", __FILE__, __LINE__, __METHOD__);
			return ($info);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					$info = FineFSManager::requestGetHead($server[0], $filename, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Get info success.", __FILE__, __LINE__, __METHOD__);
					return ($info);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Returns all data of a file (metadata + binary content).
	 * @param	string	$filename	Name of the file.
	 * @return	array	A hash with a key for each metadata, and the keys "size", "md5" and "data".
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function getData($filename) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Get data for file '$filename'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			if (($info = @parse_ini_file($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "File not found: '$filename'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("File not found: '$filename'.", IOException::NOT_FOUND);
			}
			if (!file_exists($filepath)) {
				// file's binary is not available locally - check if it's a symlink or a real file
				$fileToFetch = $filename;
				$whereToFetch = $filepath;
				if (is_link($filepath)) {
					// it's a link, we fetch its destination
					if (($dest = readlink($filepath)) === false ||
					    substr($dest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot']) {
						FineLog::fullLog("finefs", FineLog::WARN, "The link '$filename' is pointing to a bad file ($dest).", __FILE__, __LINE__, __METHOD__);
						throw new IOException("The link '$filename' is pointing to a bad file ($dest).", IOException::NOT_FOUND);
					}
					$fileToFetch = substr($dest, strlen($this->_conf['base']['dataRoot']));
					$whereToFetch = $dest;
				}
				// fetch the file from its origin or another server
				FineFSManager::mkpath(dirname($whereToFetch));
				if (!$this->_fetchFileFromOrigin($info['origin'], $fileToFetch, $whereToFetch)) {
					FineLog::fullLog("finefs", FineLog::WARN, "Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", __FILE__, __LINE__, __METHOD__);
					throw new IOException("Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", IOException::NOT_FOUND);
				}
			}
			$info['data'] = file_get_contents($filepath);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get data success.", __FILE__, __LINE__, __METHOD__);
			return ($info);
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$data = FineFSManager::requestGetData("localhost", $filename, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get data success.", __FILE__, __LINE__, __METHOD__);
			return ($data);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {   
					$data = FineFSManager::requestGetData($server[0], $filename, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Get data success.", __FILE__, __LINE__, __METHOD__);
					return ($data);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Get a file from the cluster and write it on the local filesystem.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$path		Destination path.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function getFile($filename, $path) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Get file '$filename'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			if (($info = @parse_ini_file($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "File not found: '$filename'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("File not found: '$filename'.", IOException::NOT_FOUND);
			}
			if (!file_exists($filepath)) {
				// file's binary is not available locally - check if it's a symlink or a real file
				$fileToFetch = $filename;
				$whereToFetch = $filepath;
				if (is_link($filepath)) {
					// it's a link, we fetch its destination
					if (($dest = readlink($filepath)) === false ||
					    substr($dest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot']) {
						FineLog::fullLog("finefs", FineLog::WARN, "The link '$filename' is pointing to a bad file ($dest).", __FILE__, __LINE__, __METHOD__);
						throw new IOException("The link '$filename' is pointing to a bad file ($dest).", IOException::NOT_FOUND);
					}
					$fileToFetch = substr($dest, strlen($this->_conf['base']['dataRoot']));
					$whereToFetch = $dest;
				}
				// fetch the file from its origin or another server
				FineFSManager::mkpath(dirname($whereToFetch));
				if (!$this->_fetchFileFromOrigin($info['origin'], $fileToFetch, $whereToFetch)) {
					FineLog::fullLog("finefs", FineLog::WARN, "Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", __FILE__, __LINE__, __METHOD__);
					throw new IOException("Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", IOException::NOT_FOUND);
				}
			}
			if (!@copy($filepath, $path)) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to copy '$filepath' to '$path'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("Unable to copy '$filepath' to '$path'.", IOException::UNWRITABLE);
			}
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get file success.", __FILE__, __LINE__, __METHOD__);
			return ($info);
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$data = FineFSManager::requestGetFileFromData("localhost", $filename, $path, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get file success.", __FILE__, __LINE__, __METHOD__);
			return ($data);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					$data = FineFSManager::requestGetFileFromData($server[0], $filename, $path, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Get file success.", __FILE__, __LINE__, __METHOD__);
					return ($data);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Write the content of a file to a file descriptor.
	 * @param	string		$filename	Name of the file.
	 * @param	resource	$fpOut		(optional) The file descriptor where the file's content must be written.
	 *						Would write to STDOUT if not defined.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	ApplicationException
	 * @throws	IOException
	 * @todo	Use stream_copy_to_stream instead of a read/write loop.
	 */
	public function getFileOnDescriptor($filename, $fpOut=null) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Get file '$filename' on descriptor.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			if (($info = @parse_ini_file($infopath)) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "File not found: '$filename'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("File not found: '$filename'.", IOException::NOT_FOUND);
			}
			if (!is_readable($filepath)) {
				// file's binary is not available locally - check if it's a symlink or a real file
				$fileToFetch = $filename;
				$whereToFetch = $filepath;
				if (is_link($filepath)) {
					// it's a link, we fetch its destination
					if (($dest = readlink($filepath)) === false ||
					    substr($dest, 0, strlen($this->_conf['base']['dataRoot'])) != $this->_conf['base']['dataRoot']) {
						FineLog::fullLog("finefs", FineLog::WARN, "The link '$filename' is pointing to a bad file ($dest).", __FILE__, __LINE__, __METHOD__);
						throw new IOException("The link '$filename' is pointing to a bad file ($dest).", IOException::NOT_FOUND);
					}
					$fileToFetch = substr($dest, strlen($this->_conf['base']['dataRoot']));
					$whereToFetch = $dest;
				}
				// fetch the file from its origin or another server
				FineFSManager::mkpath(dirname($whereToFetch));
				if (!$this->_fetchFileFromOrigin($info['origin'], $fileToFetch, $whereToFetch)) {
					FineLog::fullLog("finefs", FineLog::WARN, "Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", __FILE__, __LINE__, __METHOD__);
					throw new IOException("Unable to fetch file '$filename' from its origin '" . $info['origin'] . "'.", IOException::NOT_FOUND);
				}
			}
			// direct writing to standard output if needed
			if (!isset($fpOut) || $fpOut === STDOUT) {
				if (@readfile($filepath) === false) {
					FineLog::fullLog("finefs", FineLog::WARN, "Unable to read file '$filepath'.", __FILE__, __LINE__, __METHOD__);
					throw new IOException("Unable to read file '$filepath'.", IOException::UNREADABLE);
				}
				return ($info);
			}
			// open and write file
			if (($fpIn = fopen($filepath, "r")) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to read file '$filepath'.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("Unable to read file '$filepath'.", IOException::UNREADABLE);
			}
			while (!feof($fpIn)) {
				$chunk = fread($fpIn, 8192);
				if (fwrite($fpOut, $chunk) === false) {
					FineLog::fullLog("finefs", FineLog::WARN, "Unable to write on file descriptor.", __FILE__, __LINE__, __METHOD__);
					throw new IOException("Unable to write on file descriptor.", IOException::UNWRITABLE);
				}
			}
			fclose($fpIn);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get file on descriptor success.", __FILE__, __LINE__, __METHOD__);
			return ($info);
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			$data = FineFSManager::requestGetDataToFileDescriptor("localhost", $filename, $fp, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Get file on descriptor success.", __FILE__, __LINE__, __METHOD__);
			return ($data);
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					$data = FineFSManager::requestGetDataToFileDescriptor($server[0], $filename, $fpOut, true, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Get file on descriptor success.", __FILE__, __LINE__, __METHOD__);
					return ($data);
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Remove a file from the cluster.
	 * @param	string	$filename	Name of the file.
	 * @throws	ApplicationException
	 * @throws	IOException
	 */
	public function remove($filename) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Remove file '$filename'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			$info = @parse_ini_file($infopath);
			@unlink($filepath);
			@unlink($infopath);
			// delete empty directories
			$excludeDirs = array($this->_conf['base']['dataRoot'], $this->_conf['base']['infoRoot']);
			FineFSManager::rmEmptyDir(dirname($filepath), $excludeDirs);
			FineFSManager::rmEmptyDir(dirname($infopath), $excludeDirs);
			// forward the request to the next server
			$this->_sendDelToNextPeer($filename);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remove success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			FineFSManager::requestDel("localhost", $filename, null, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remove success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					FineFSManager::requestDel($server[0], $filename, null, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Remove success.", __FILE__, __LINE__, __METHOD__);
					return;
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}
	/**
	 * Rename a file on the cluster.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$newname	New name of the file.
	 * @throws	ApplicationException
	 */
	public function rename($filename, $newname) {
		FineLog::fullLog("finefs", FineLog::DEBUG, "Rename file '$filename' to '$newname'.", __FILE__, __LINE__, __METHOD__);
		if ($this->_useLocalData) {
			// local server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Direct access.", __FILE__, __LINE__, __METHOD__);
			$infopath = $this->_conf['base']['infoRoot'] . "/" . $filename;
			$filepath = $this->_conf['base']['dataRoot'] . "/" . $filename;
			// check the destination name
			$newname = trim($newname);
			$newname = trim($newname, '/');
			if (strpos($newname, "..") !== false) {
				FineLog::fullLog("finefs", FineLog::ERROR, "Forbidden path '$newname'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Forbidden path '$newname'.", IOException::BAD_FORMAT);
			}
			$newfilepath = $this->_conf['base']['dataRoot'] . "/" . $newname;
			$newinfopath = $this->_conf['base']['infoRoot'] . "/" . $newname;
			// get the metadata file
			$info = @parse_ini_file($infopath);
			if ($info === false || empty($info)) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to find file '$infopath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to find file '$infopath'.", IOException::NOT_FOUND);
			}
			// check that the binary content is locally available and is not a symlink
			if (!file_exists($filepath) && !is_link($filepath)) {
				// it's not, we fetch it
				FineFSManager::mkpath(dirname($filepath));
				if (!$this->_fetchFileUpToOrigin($info['origin'], $filename, $filepath, false)) {
					FineLog::fullLog("finefs", FineLog::ERROR, "Unable to get file '$filename' from any server up to its origin.", __FILE__, __LINE__, __CLASS__);
					throw new IOException("Unable to get file '$filename' from any server up to its origin.", IOException::NOT_FOUND);
				}
			}
			// new paths creation
			FineFSManager::mkpath(dirname($newfilepath));
			FineFSManager::mkpath(dirname($newinfopath));
			// moving files
			if (!@rename($infopath, $newinfopath)) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename file '$infopath' to '$newinfopath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to rename file '$infopath' to '$newinfopath'.", IOException::NOT_FOUND);
			}
			if (!@rename($filepath, $newfilepath)) {
				@rename($newinfopath, $infopath);
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename file '$filepath' to '$newfilepath'.", __FILE__, __LINE__, __CLASS__);
				throw new IOException("Unable to rename file '$filepath' to '$newfilepath'.", IOException::NOT_FOUND);
			}
			// delete empty directories
			$excludeDirs = array($this->_conf['base']['dataRoot'], $this->_conf['base']['infoRoot']);
			FineFSManager::rmEmptyDir(dirname($filepath), $excludeDirs);
			FineFSManager::rmEmptyDir(dirname($infopath), $excludeDirs);
			// forward the request to the next peer
			$this->_sendRenameToNextPeer($filename, $newname);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Rename success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useLocalDaemon) {
			// local daemon
			FineLog::fullLog("finefs", FineLog::DEBUG, "Local daemon.", __FILE__, __LINE__, __METHOD__);
			FineFSManager::requestRename("localhost", $filename, $newname, null, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
			FineLog::fullLog("finefs", FineLog::DEBUG, "Rename success.", __FILE__, __LINE__, __METHOD__);
			return;
		} else if ($this->_useDistantPeer) {
			// remote server
			FineLog::fullLog("finefs", FineLog::DEBUG, "Remote server.", __FILE__, __LINE__, __METHOD__);
			$peers = $this->_conf['addresses']['peers'];
			while (count($peers)) {
				$server = array_splice($peers, mt_rand(0, (count($peers) - 1)), 1);
				if (in_array($server[0], $this->_conf['addresses']['disabled']))
					continue;
				try {
					FineFSManager::requestRename($server[0], $filename, $newname, null, $this->_conf['base']['port'],
								     $this->_conf['base']['timeout']);
					FineLog::fullLog("finefs", FineLog::DEBUG, "Rename success.", __FILE__, __LINE__, __METHOD__);
					return;
				} catch (Exception $e) { }
			}
		}
		// no one available server
		FineLog::fullLog("finefs", FineLog::WARN, "No usable peer to connect to.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("No usable peer to connect to.", ApplicationException::UNKNOWN);
	}

	/* ***************** PRIVATE METHODS ****************** */
	/**
	 * Fetch a file from its origin server, or any server after.
	 * @param	string	$origin		Name of the origin server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$filepath	Local path of writing.
	 * @param	bool	$transfer	(optional) Send the file to next server, or not. True by default.
	 * @return	bool	True if the file was correctly fetched, False otherwise.
	 */
	private function _fetchFileFromOrigin($origin, $filename, $filepath, $transfer=true) {
		if (!$this->_useDistantPeer) {
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
					if (!rename($tmpPath, $filepath)) {
						FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename '$tmpPath' to '$filepath'.", __FILE__, __LINE__, __CLASS__);
						@unlink($tmpPath);
						throw new IOException("", IOException::UNWRITABLE);
					}
					if ($transfer)
						FineFSManager::addFileToProcessLog($filename);
					FineLog::fullLog("finefs", FineLog::DEBUG, "File '$filename' fetched.", __FILE__, __LINE__, __METHOD__);
					return (true);
				} catch (Exception $e) {
					FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to fetch file '$filename' from server '$peer'.", __FILE__, __LINE__, __CLASS__);
				}
			}
		}
		FineLog::fullLog("finefs", FineLog::WARN, "Unable to fetch file '$filename'.", __FILE__, __LINE__, __METHOD__);
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
		if (!$this->_useDistantPeer) {
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
				if (!rename($tmpPath, $filepath)) {
					FineLog::fullLog("finefs", FineLog::INFO, "Unable to rename '$tmpPath' to '$filepath'.", __FILE__, __LINE__, __CLASS__);
					@unlink($tmpPath);
					throw new IOException("", IOException::UNWRITABLE);
				}
				if ($transfer)
					FineFSManager::addFileToProcessLog($filename);
				FineLog::fullLog("finefs", FineLog::DEBUG, "File '$filename' fetched.", __FILE__, __LINE__, __METHOD__);
				return (true);
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::DEBUG, "Unable to fetch file '$filename' from server '$peer'.", __FILE__, __LINE__, __CLASS__);
			}
			if ($peer == $origin)
				break;
		}
		FineLog::fullLog("finefs", FineLog::WARN, "Unable to fetch file '$filename'.", __FILE__, __LINE__, __METHOD__);
		return (false);
	}
	/**
	 * Send a PUTHEAD request to the next server.
	 * @param	string	$filename	Name of the file to send.
	 * @param	array	$info		File's metadata.
	 */
	private function _sendPutHeadToNextPeer($filename, $info) {
		if (!$this->_useDistantPeer)
			return;
		$info['origin'] = $this->_conf['addresses']['local'];
		// send metadata to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled']))
				continue;
			try {   
				FineFSManager::requestPutHead($peer, $filename, $info, $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send PUTHEAD request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
			}
		}
	}
	/**
	 * Send a DEL request to the next server.
	 * @param	string	$filename	Name of the file to delete.
	 */
	private function _sendDelToNextPeer($filename) {
		if (!$this->_useDistantPeer)
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("del", $peer, $filename);
				continue;
			}
			try {
				FineFSManager::requestDel($peer, $filename, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send DEL request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("del", $peer, $filename);
			}
		}
	}
	/**
	 * Send a RENAME request to the next server.
	 * @param	string	$filename	Name of the file to rename.
	 * @param	string	$newfile	New name of the file.
	 */
	private function _sendRenameToNextPeer($filename, $newfile) {
		if (!$this->_useDistantPeer)
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("ren", $peer, $filename, $newfile);
				continue;
			}
			try {   
				FineFSManager::requestRename($peer, $filename, $newfile, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send RENAME request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("ren", $peer, $filename, $newfile);
			}
		}
	}
	/**
	 * Send a LINK request to the next server.
	 * @param	string	$filename	Name of the link to create.
	 * @param	string	$target		Name of the file to link to.
	 */
	private function _sendLinkToNextPeer($filename, $target) {
		if (!$this->_useDistantPeer)
			return;
		// send the request to the next server
		foreach ($this->_conf['addresses']['peers'] as $peer) {
			if (in_array($peer, $this->_conf['addresses']['disabled'])) {
				FineFSManager::addFileToErrorLog("lnk", $peer, $filename, $target);
				continue;
			}
			try {
				FineFSManager::requestLink($peer, $filename, $target, $this->_conf['addresses']['local'], $this->_conf['base']['port'], $this->_conf['base']['timeout']);
				break;
			} catch (Exception $e) {
				FineLog::fullLog("finefs", FineLog::INFO, "Unable to send LINK request to server '$peer'.", __FILE__, __LINE__, __CLASS__);
				FineFSManager::addFileToErrorLog("lnk", $peer, $filename, $target);
			}
		}
	}
}

?>
