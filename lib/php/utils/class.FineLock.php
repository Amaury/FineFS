<?php

if (!class_exists("FineLock")) {

require_once(dirname(__FILE__) . "/except.IOException.php");

/**
 * Lock management object.
 *
 * By default, this object tries to lock the PHP script currently running. But it's
 * possible to lock another file.
 * Here is a simple example, to prevent concurrent executions of the same program:
 * <code>
 * $lock = new FineLock();
 * try {
 *     $lock->lock();
 *     // processing...
 *     $lock->unlock();
 * } catch (IOException $e) {
 *     // IO error, possibly from the lock
 * } catch ($e) {
 *     // error
 * }
 * </code>
 * If the file is already locked, we check the lock created date. If it's older than
 * the default timeout (10 minutes), we checks is the process which creates the lock
 * is still alive. If not, we try to unlock and relock.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2008, FineMedia
 * @package	FineFS
 * @subpackage	utils
 * @version	$Id$
 */
class FineLock {
	/** Constant - lock files' name suffix. */
	const LOCK_SUFFIX = ".lck";
	/** Constant - default lock timeout (in seconds). 10 minutes by default. */
	const LOCK_TIMEOUT = 600;
	/** Lock file handler. */
	private $_fileHandle = null;
	/** Path to the lock file. */
	private $_lockPath = null;

	/**
	 * Lock creation.
	 * @param	string	$path		(optional) Full path of the file to lock.
	 *					By default, locks the current running program.
	 * @param	int	$timeout	(optional) Lock timeout allowed on this file, in seconds.
	 * @throws	IOException
	 */
	public function lock($path=null, $timeout=null) {
		$filePath = is_null($path) ? $_SERVER["SCRIPT_FILENAME"] : $path;
		$lockPath = $filePath . self::LOCK_SUFFIX;
		$this->_lockPath = $lockPath;
		if (!($this->_fileHandle = @fopen($this->_lockPath, "a+"))) {
			$lockPath = $this->_lockPath;
			$this->_reset();
			throw new IOException("Unable to open file '$lockPath'.", IOException::UNREADABLE);
		}
		if (!flock($this->_fileHandle, LOCK_EX + LOCK_NB)) {
			// unable to lock: check the lock date
			if (($stat = stat($this->_lockPath)) !== false) {
				$usedTimeout = is_null($timeout) ? self::LOCK_TIMEOUT : $timeout;
				if (($stat["ctime"] + $usedTimeout) < time()) {
					// expired timeout: check if the process still exists
					$pid = trim(file_get_contents($this->_lockPath));
					$cmd = "ps -p " . escapeshellarg($pid) . " | wc -l";
					$nbr = trim(shell_exec($cmd));
					if ($nbr < 2) {
						// the process doesn't exists anymore: try to unlock and relock
						$this->unlock();
						$this->lock($filePath, $usedTimeout);
						return;
					}
				}
			}
			fclose($this->_fileHandle);
			$this->_reset();
			throw new IOException("Unable to lock file '$lockPath'.", IOException::UNLOCKABLE);
		}
		// file locked: write the PID on the lock file
		ftruncate($this->_fileHandle, 0);
		fwrite($this->_fileHandle, getmypid());
	}
	/**
	 * Lock release.
	 * @throws	IOException
	 */
	public function unlock() {
		if (is_null($this->_fileHandle) || is_null($this->_lockPath)) {
			throw new IOException("No file to unlock.", IOException::NOT_FOUND);
		}
		flock($this->_fileHandle, LOCK_UN);
		if (!fclose($this->_fileHandle)) {
			$this->_reset();
			throw new IOException("Unable to close lock file.", IOException::FUNDAMENTAL);
		}
		if (!unlink($this->_lockPath)) {
			$this->_reset();
			throw new IOException("Unable to delete lock file.", IOException::FUNDAMENTAL);
		}
		$this->_reset();
	}
	/** Private attributes flushing. */
	private function _reset() {
		$this->_fileHandle = null;
		$this->_lockPath = null;
	}
}

} // class_exists

?>
