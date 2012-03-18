<?php

$fineFSroot = empty($fineFSroot) ? (dirname(__FILE__) . "/../..") : $fineFSroot;

require_once("$fineFSroot/lib/php/utils/class.FineLog.php");
require_once("$fineFSroot/lib/php/utils/except.ApplicationException.php");
require_once("$fineFSroot/lib/php/utils/except.IOException.php");

/**
 * Management of communication with FineFS servers.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, FineMedia
 * @package	FineFS
 * @subpackage	lib
 * @version	$Id$
 */
class FineFSManager {
	/** Constant - Path to the metadata files tree, from the FineFS root directory. */
	const FINEFS_INFO_PATH = "var/info";
	/** Constant - Path to the binary files tree, from the FineFS root directory. */
	const FINEFS_DATA_PATH = "var/data";
	/** Constant - Path to the temporary directory, from the FineFS root directory. */
	const FINEFS_TMP_PATH = "var/tmp";
	/** Constant - Default FineFS user. */
	const FINEFS_DEFAULT_USER = "www-data";
	/** Constant - Default FineFS log level. */
	const FINEFS_DEFAULT_LOGLEVEL = "WARN";
	/** Constant - Default FineFS port. */
	const FINEFS_DEFAULT_PORT = 11137;
	/** Constant - Default FineFS timeout (in seconds). */
	const FINEFS_DEFAULT_TIMEOUT = 2;

	/* **************** CONNECTION METHODS **************** */
	/**
	 * Send a HELO request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$localhost	Name of the local machine.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in seconds. 3 by default.
	 * @return	array	A hash with keys 'protocol', 'server' and 'client'.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestHelo($server, $localhost, $port=11137, $timeout=3) {
		$localhost = empty($localhost) ? "-" : $localhost;
		// request creation
		$out = "HELO $localhost\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		$info = array();
		while (!feof($sock)) {
			$line = trim(fgets($sock));
			if (empty($line))
				break;
			if (($pos = strpos($line, ":")) !== false) {
				$key = trim(substr($line, 0, $pos));
				$val = trim(substr($line, $pos + 1));
				$info[$key] = $val;
			}
		}
		fclose($sock);
		FineLog::fullLog("finefs", FineLog::DEBUG, "HELO success.", __FILE__, __LINE__, __METHOD__);
		return ($info);
	}
	/**
	 * Send a PUTHEAD request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	array	$info		(optional) Hash containing file's metadata.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in seconds. 3 by default.
	 * @return	bool	True if the request works.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestPutHead($server, $filename, $info=null, $port=11137, $timeout=3) {
		// request creation
		$out = "PUTHEAD $filename\r\n";
		if (is_array($info))
			foreach ($info as $key => $val)
				$out .= "$key: $val\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		fclose($sock);
		if ($response == "OK") {
			FineLog::fullLog("finefs", FineLog::DEBUG, "PUTHEAD success.", __FILE__, __LINE__, __METHOD__);
			return (true);
		}
		FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}
	/**
	 * Send a LINK request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the link.
	 * @param	string	$target		Target of the link.
	 * @param	string	$origin		(optional) Name of the originator server.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	bool	True if the request works.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestLink($server, $filename, $target, $origin="", $port=11137, $timeout=3)  {
		// request creation
		$out = "LINK $filename\r\n";
		$out .= "target: $target\r\n";
		if (!empty($origin))
			$out .= "origin: $origin\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		// send request's header
		fwrite($sock, $out);
		// get server's response
		$response = trim(fgets($sock));
		if ($response == "OK") {
			FineLog::fullLog("finefs", FineLog::DEBUG, "LINK success.", __FILE__, __LINE__, __METHOD__);
			return (true);
		}
		fclose($sock);
		FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}
	/**
	 * Send a PUTDATA request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	binary	$data		Binary content of the file.
	 * @param	array	$info		(optional) Hash containing file's metadata.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	bool	True if the request works.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestPutData($server, $filename, $data, $info=null, $port=11137, $timeout=3) {
		$contentMD5 = md5($data);
		$datasize = mb_strlen($data, "ASCII");
		// request creation
		$out = "PUTDATA $filename\r\n";
		if (is_array($info))
			foreach ($info as $key => $val)
				$out .= "$key: $val\r\n";
		$out .= "md5: $contentMD5\r\n";
		$out .= "size: $datasize\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		// send request's header
		fwrite($sock, $out);
		// get server's response
		$response = trim(fgets($sock));
		if ($response == "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success. No data sent.", __FILE__, __LINE__, __METHOD__);
			return (true);
		} else if ($response == "GO") {
			// send data
			fwrite($sock, $data);
			// get server's response
			$response = trim(fgets($sock));
			if ($response == "OK") {
				fclose($sock);
				FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success.", __FILE__, __LINE__, __METHOD__);
				return (true);
			}
		}
		fclose($sock);
		FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}
	/**
	 * Send a PUTDATA request to a FineFS server, using the content of a local file.
	 * @param	string	$server		Name of the server
	 * @param	string	$filename	Name of the file.
	 * @param	string	$localpath	Path to the local file.
	 * @param	array	$info		(optional) Hash containing file's metadata.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	bool	True if the request works.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestPutDataFromFile($server, $filename, $localpath, $info=null, $port=11137, $timeout=3) {
		// request creation
		$contentMD5 = md5_file($localpath);
		$filesize = filesize($localpath);
		$out = "PUTDATA $filename\r\n";
		if (is_array($info))
			foreach ($info as $key => $val)
				$out .= "$key: $val\r\n";
		$out .= "md5: $contentMD5\r\n";
		$out .= "size: $filesize\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response == "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success. No data sent.", __FILE__, __LINE__, __METHOD__);
			return (true);
		} else if ($response != "GO") {
			FineLog::fullLog("finefs", FineLog::INFO, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __CLASS__);
			fclose($sock);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		// open the local file
		if (($fp = fopen($localpath, "r")) === false) {
			FineLog::fullLog("finefs", FineLog::INFO, "Unable to read file '$localpath'.", __FILE__, __LINE__, __CLASS__);
			throw new IOException("Unable to read file '$localpath'.", IOException::UNREADABLE);
		}
		// send file's content
		while (!feof($fp)) {
			$chunk = fread($fp, 8192);
			fwrite($sock, $chunk);
		}
		// get server's response
		$response = trim(fgets($sock));
		fclose($sock);
		if ($response == "OK") {
			FineLog::fullLog("finefs", FineLog::DEBUG, "PUTDATA success.", __FILE__, __LINE__, __METHOD__);
			return (true);
		}
		FineLog::fullLog("finefs", FineLog::INFO, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __CLASS__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}
	/**
	 * Send a LIST request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$path		Search path (with stars).
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	array	List of hashes.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestList($server, $path, $port=11137, $timeout=3) {
		// request creation
		$out .= "LIST $path\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		$inHeader = true;
		$step = 0;
		$filename = "";
		$info = array();
		$result = array();
		while (!feof($sock)) {
			$line = trim(fgets($sock));
			if ($inHeader) {
				if (empty($line)) {
					$inHeader = false;
					continue;
				}
			} else {
				if (empty($filename)) {
					// new file
					$filename = $line;
				} else if (empty($line)) {
					// end of file processing
					$result[$filename] = $info;
					$filename = "";
					$info = array();
				} else {
					// metadata
					if (($pos = strpos($line, ":")) !== false) {
						$key = trim(substr($line, 0, $pos));
						$val = trim(substr($line, $pos + 1));
						$info[$key] = $val;
					}
				}
			}
		}
		fclose($sock);
		FineLog::fullLog("finefs", FineLog::DEBUG, "LIST success.", __FILE__, __LINE__, __METHOD__);
		return ($result);
	}
	/**
	 * Send a GETHEAD request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestGetHead($server, $filename, $port=11137, $timeout=3) {
		// request creation
		$out = "GETHEAD $filename\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		$info = array();
		while (!feof($sock)) {
			$line = trim(fgets($sock));
			if (empty($line))
				break;
			if (($pos = strpos($line, ":")) !== false) {
				$key = trim(substr($line, 0, $pos));
				$val = trim(substr($line, $pos + 1));
				$info[$key] = $val;
			}
		}
		fclose($sock);
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETHEAD success.", __FILE__, __LINE__, __METHOD__);
		return ($info);
	}
	/**
	 * Send a GETDATA request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	bool	$followup	(optional) Should try to get the file on another server if needed. True by default.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	array	A hash with a key for each metadata, and the keys "size", "md5" and "data".
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestGetData($server, $filename, $followup=true, $port=11137, $timeout=3) {
		// request creation
		$out = "GETDATA $filename\r\n";
		if ($followup === false)
			$out .= "nofollowup: 1\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		$info = array(
			'data'	=> ''
		);
		$inHeaders = true;
		$body = "";
		while (!feof($sock)) {
			$line = fgets($sock);
			if ($inHeaders) {
				if (trim($line) == "") {
					$inHeaders = false;
					continue;
				}
				if (($pos = strpos($line, ":")) !== false) {
					$key = trim(substr($line, 0, $pos));
					$val = trim(substr($line, $pos + 1));
					$info[$key] = $val;
				}
			} else
				$info["data"] .= $line;
		}
		fclose($sock);
		$receivedDataSize = mb_strlen($info["data"], "ASCII");
		if ($receivedDataSize != $info["size"]) {
			FineLog::fullLog("finefs", FineLog::WARN, "Received '$receivedDataSize' bytes of data, instead of '" . $info["size"] . "'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Received '$receivedDataSize' bytes of data, instead of '" . $info["size"] . "'.", IOException::UNREADABLE);
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETDATA success.", __FILE__, __LINE__, __METHOD__);
		return ($info);
	}
	/**
	 * Send a GETDATA request to a FineFS server, and write the received content to a local file.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$path		Path of the local written file.
	 * @param	bool	$followup	(optional) Should try to get the file on another server if needed. True by default.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestGetFileFromData($server, $filename, $path, $followup=true, $port=11137, $timeout=3) {
		// open the local destination file
		if (($fp = fopen($path, "w")) === false) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open file '$path'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open file '$path'.", IOException::UNWRITABLE);
		}
		// request creation
		$out = "GETDATA $filename\r\n";
		if ($followup === false)
			$out .= "nofollowup: 1\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		// get data
		$info = array();
		$inHeaders = true;
		$body = "";
		while ($inHeaders && !feof($sock)) {
			$line = fgets($sock);
			if (trim($line) == "") {
				$inHeaders = false;
				continue;
			}
			if (($pos = strpos($line, ":")) !== false) {
				$key = trim(substr($line, 0, $pos));
				$val = trim(substr($line, $pos + 1));
				$info[$key] = $val;
			}
		}
		while (!feof($sock)) {
			$chunk = fread($sock, 8192);
			fwrite($fp, $chunk);
		}
		fclose($sock);
		fclose($fp);
		$receivedDataSize = filesize($path);
		$receivedMD5 = md5_file($path);
		if ($receivedDataSize != $info['size'] || $receivedMD5 != $info['md5']) {
			unlink($path);
			FineLog::fullLog("finefs", FineLog::WARN, "Received corrupted data for file '$filename'.", __FILE__, __LINE__, __CLASS__);
			throw new IOException("Received corrupted data for file '$filename'.", IOException::BAD_FORMAT);
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETDATA success.", __FILE__, __LINE__, __METHOD__);
		return ($info);
	}
	/**
	 * Send a GETDATA request to a FineFS server, and write the received content on a given file descriptor.
	 * @param	string		$server		Name of the server.
	 * @param	string		$filename	Name of the file.
	 * @param	resource	$fd		(optional) Destination file descriptor. STDOUT if not defined.
	 * @param	bool		$followup	(optional) Should try to get the file on another server if needed. True by default.
	 * @param	int		$port		(optional) Connection's port number. 11137 by default.
	 * @param	int		$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	array	A hash with a key for each metadata, and the keys "size" and "md5".
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestGetDataToFileDescriptor($server, $filename, $fd=null, $followup=true, $port=11137, $timeout=3) {
		// request creation
		$out = "GETDATA $filename\r\n";
		if ($followup === false)
			$out .= "nofollowup: 1\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		if ($response != "OK") {
			fclose($sock);
			FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
			throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
		}
		// get data
		$info = array();
		$inHeaders = true;
		$body = "";
		while ($inHeaders && !feof($sock)) {
			$line = fgets($sock);
			if (trim($line) == "") {
				$inHeaders = false;
				continue;
			}
			if (($pos = strpos($line, ":")) !== false) {
				$key = trim(substr($line, 0, $pos));
				$val = trim(substr($line, $pos + 1));
				$info[$key] = $val;
			}
		}
		$receivedDataSize = 0;
		$hashCtx = hash_init('md5');
		while (!feof($sock)) {
			$chunk = fread($sock, 8192);
			$receivedDataSize += mb_strlen($chunk, "ASCII");
			hash_update($hashCtx, $chunk);
			if (!isset($fd))
				print($chunk);
			else if (@fwrite($fd, $chunk) === false) {
				FineLog::fullLog("finefs", FineLog::WARN, "Unable to write on file descriptor.", __FILE__, __LINE__, __METHOD__);
				throw new IOException("Unable to write on file descriptor.", IOException::UNWRITABLE);
			}
		}
		fclose($sock);
		$receivedMD5 = hash_final($hashCtx);
		if ($receivedDataSize != $info['size'] || $receivedMD5 != $info['md5']) {
			unlink($path);
			FineLog::fullLog("finefs", FineLog::WARN, "Received corrupted data for file '$filename'.", __FILE__, __LINE__, __CLASS__);
			throw new IOException("Received corrupted data for file '$filename'.", IOException::BAD_FORMAT);
		}
		FineLog::fullLog("finefs", FineLog::DEBUG, "GETDATA success.", __FILE__, __LINE__, __METHOD__);
		return ($info);
	}
	/**
	 * Send a DEL request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$origin		(optional) Name of the originator server.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	bool	True if the deletion was successfully done.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestDel($server, $filename, $origin="", $port=11137, $timeout=3) {
		// request creation
		$out = "DEL $filename\r\n";
		if (!empty($origin))
			$out .= "origin: $origin\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		fclose($sock);
		if ($response == "OK") {
			FineLog::fullLog("finefs", FineLog::DEBUG, "DEL success.", __FILE__, __LINE__, __METHOD__);
			return (true);
		}
		FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}
	/**
	 * Send a RENAME request to a FineFS server.
	 * @param	string	$server		Name of the server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$newname	New name of the file.
	 * @param	string	$origin		(optional) Name of the originator server.
	 * @param	int	$port		(optional) Connection's port number. 11137 by default.
	 * @param	int	$timeout	(optional) Connection's timeout delay, in second. 3 by default.
	 * @return	bool	True if the deletion was successfully done.
	 * @throws	IOException		If the server wasn't available.
	 * @throws	ApplicationException	If the server returns an incorrect answer.
	 */
	static public function requestRename($server, $filename, $newname, $origin="", $port=11137, $timeout=3) {
		// request creation
		$out = "RENAME $filename\r\n";
		$out .= "newfile: $newname\r\n";
		if (!empty($origin))
			$out .= "origin: $origin\r\n";
		$out .= "\r\n";
		// connection
		if (!($sock = @fsockopen($server, $port, $errno, $errstr, $timeout))) {
			FineLog::fullLog("finefs", FineLog::WARN, "Unable to open socket to server '$server'.", __FILE__, __LINE__, __METHOD__);
			throw new IOException("Unable to open socket to server '$server'.", IOException::NOT_FOUND);
		}
		fwrite($sock, $out);
		$response = trim(fgets($sock));
		fclose($sock);
		if ($response == "OK") {
			FineLog::fullLog("finefs", FineLog::DEBUG, "RENAME success.", __FILE__, __LINE__, __METHOD__);
			return (true);
		}
		FineLog::fullLog("finefs", FineLog::WARN, "Bad response from server '$server': '$response'.", __FILE__, __LINE__, __METHOD__);
		throw new ApplicationException("Bad response from server '$server': '$response'.", ApplicationException::SYSTEM);
	}

	/* ************************** LOCAL MANAGEMENT METHODS ******************** */
	/**
	 * Add a file to the "new files" log.
	 * @param	string	$filename	Name of the file.
	 */
	static public function addFileToProcessLog($filename) {
		global $fineFSroot;

		$fineFSroot = empty($fineFSroot) ? (dirname(__FILE__) . "/../..") : $fineFSroot;
		FineLog::fullLog("finefs", FineLog::DEBUG, "Add file '$filename' to 'new files' log.", __FILE__, __LINE__, __METHOD__);
		$path = "$fineFSroot/log/filestoprocess/" . uniqid(time() . "-", true);
		$content = $filename;
		file_put_contents($path, $content);
	}
	/**
	 * Add a file to the "error" log.
	 * @param	string	$type		Request type: "add", "del" or "ren".
	 * @param	string	$server		Name of the unavailable server.
	 * @param	string	$filename	Name of the file.
	 * @param	string	$newname	(optional) New name of the file, if it was a renaming operation.
	 */
	static public function addFileToErrorLog($type, $server, $filename, $newname="") {
		global $fineFSroot;

		$fineFSroot = empty($fineFSroot) ? (dirname(__FILE__) . "/../..") : $fineFSroot;
		FineLog::fullLog("finefs", FineLog::DEBUG, "Add file '$filename' to 'errors' log. Command '$type'.", __FILE__, __LINE__, __METHOD__);
		$path = "$fineFSroot/log/errorstoprocess/" . uniqid(time() . "-", true);
		$content = $server . "\r\n";
		$content .= strtoupper($type) . "\r\n";
		$content .= $filename . "\r\n";
		if ($type == "ren" || $type == "lnk")
			$content .= $newname . "\r\n";
		file_put_contents($path, $content);
	}
	/**
	 * Extract the parameters of a request.
	 * @param	resource	$sock	(optional) Input socket. STDIN by default.
	 * @return	array	Le hash des paramètres.
	 */
	static public function extractParams($sock=null) {
		$sock = is_null($sock) ? STDIN : $sock;
		$params = array();
		while (!feof($sock)) {
			$line = trim(fgets($sock));
			if (empty($line))
				break;
			if (($pos = strpos($line, ":")) !== false) {
				$key = trim(substr($line, 0, $pos));
				$val = trim(substr($line, $pos + 1));
				$params[$key] = $val;
			}
		}
		return ($params);
	}
	/**
	 * Recursively creates the directories of a path.
	 * @param	string	$path	The path to create.
	 * @return	bool	True is the path exists or has been successfully created.
	 */
	static public function mkpath($path) {
		is_dir(dirname($path)) || self::mkpath(dirname($path));
		return (is_dir($path) || @mkdir($path));
	}
	/**
	 * Recursively remove an empty directory and its parents.
	 * @param	string	$path		The path to delete.
	 * @param	array	$exclude	(optional) List of paths that must not be deleted.
	 */
	static public function rmEmptyDir($path, $exclude=null) {
		if (!is_dir($path) ||
		    (is_array($exclude) && in_array($path, $exclude)) ||
		    @rmdir($path) !== true)
			return;
		$parentDir = dirname($path);
		self::rmEmptyDir($parentDir);
	}
	/**
	 * Create the content of an INI file.
	 * @param	array	$data	Hash with key/value data.
	 * @return	string	The content of the resulting INI file.
	 */
	static public function encodeIniFile($data) {
		$str = "";
		foreach ($data as $key => $val) {
			if (is_null($val))
				continue;
			if (is_array($val)) {
				foreach ($val as $sub) {
					$str .= $key . "[]=";
					if (is_bool($sub))
						$str .= ($sub ? "true" : "false") . "\n";
					else
						$str .= "\"$sub\"\n";
				}
			} else if (is_bool($val))
				$str .= "$key=" . ($val ? "true" : "false") . "\n";
			else
				$str .= "$key=\"$val\"\n";
		}
		return ($str);
	}
	/**
	 * Read a FineFS configuration file.
	 * @param	mixed	$conf	(optional) Path to the configuration file, or textual content of the configuration (INI format).
	 *				By default, the configuration is read from file 'etc/finefs.ini'.
	 * @return	array	Hash of configuration information.
	 */
	static public function readConfiguration($conf=null) {
		global $fineFSroot;

		$fineFSroot = empty($fineFSroot) ? (dirname(__FILE__) . "/../..") : $fineFSroot;
		if (!empty($conf)) {
			if (is_file($conf))
				$conf = @parse_ini_file($conf, true);
			else
				$conf = @parse_ini_string($conf, true);
		}
		if ($conf === false || $conf === null)
			$conf = @parse_ini_file("$fineFSroot/etc/finefs.ini", true);
		if (!is_array($conf))
			$conf = array();
		if (!is_array($conf['base']))
			$conf['base'] = array();
		if (!is_array($conf['addresses']['peers']))
			$conf['addresses']['peers'] = array();
		if (!isset($conf['addresses']['disabled']) || !is_array($conf['addresses']['disabled']))
			$conf['addresses']['disabled'] = array();
		// default values
		if (empty($conf['base']['user']))
			$conf['base']['user'] = self::FINEFS_DEFAULT_USER;
		if (empty($conf['base']['loglevel']))
			$conf['base']['loglevel'] = self::FINEFS_DEFAULT_LOGLEVEL;
		if (empty($conf['base']['port']))
			$conf['base']['port'] = self::FINEFS_DEFAULT_PORT;
		if (!isset($conf['base']['timeout']) || !is_numeric($conf['base']['timeout']))
			$conf['base']['timeout'] = self::FINEFS_DEFAULT_TIMEOUT;
		// paths to data
		if (empty($conf['base']['dataRoot']))
			$conf['base']['dataRoot'] = realpath("$fineFSroot/" . FineFSManager::FINEFS_DATA_PATH);
		else if (substr($conf['base']['dataRoot'], 0, 1) != "/")
			$conf['base']['dataRoot'] = realpath("$fineFSroot/" . $conf['base']['dataRoot']);
		if (empty($conf['base']['infoRoot']))
			$conf['base']['infoRoot'] = realpath("$fineFSroot/" . FineFSManager::FINEFS_INFO_PATH);
		else if (substr($conf['base']['infoRoot'], 0, 1) != "/")
			$conf['base']['infoRoot'] = realpath("$fineFSroot/" . $conf['base']['infoRoot']);
		if (empty($conf['base']['tmpRoot']))
			$conf['base']['tmpRoot'] = realpath("$fineFSroot/" . FineFSManager::FINEFS_TMP_PATH);
		else if (substr($conf['base']['tmpRoot'], 0, 1) != "/")
			$conf['base']['tmpRoot'] = realpath("$fineFSroot/" . $conf['base']['tmpRoot']);
		return ($conf);
	}
}

?>
