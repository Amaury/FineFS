#!/usr/bin/php
<?php

$fineFSroot = dirname(__FILE__) . "/..";

require_once("$fineFSroot/lib/php/utils/class.FineLog.php");
require_once("$fineFSroot/lib/php/class.FineFSManager.php");

/**
 * Program used to check if all binary files of a FineFS node are consistant with their metdata files.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, FineMedia
 * @package	FineFS
 * @subpackage	bin
 * @version	$Id$
 */

// read configuration
$conf = FineFSManager::readConfiguration();
// log definition
FineLog::setLogFile("$fineFSroot/log/checker.log");
FineLog::setThreshold(FineLog::DEBUG);
$thresholds = array(
	"default"	=> FineLog::WARN,
	"finefs"	=> eval("return (FineLog::" . $conf['base']['loglevel'] . ");")
);
FineLog::setThreshold($thresholds);

// opening
$datapath = $conf['base']['dataRoot'];
$infopath = $conf['base']['infoRoot'];
$chunkpath = "";
if (!empty($_SERVER['argv'][1])) {
	$chunkpath = $_SERVER['argv'][1];
	$datapath .= "/$chunkpath";
	$infopath .= "/$chunkpath";
}
print("CHECK	'$datapath'\n");
if (($dir = opendir($datapath)) === false) {
	print("Unable to open directory '$datapath'.\n");
	exit(1);
}
// directory scanning
while (($file = readdir($dir))) {
	if (substr($file, 0, 1) == ".")
		continue;
	$datafilepath = "$datapath/$file";
	$chunkfilepath = "$chunkpath/$file";
	$chunkfilepath = ltrim($chunkfilepath, '/');
	if (is_dir($datafilepath)) {
		$cmd = $_SERVER['argv'][0] . " " . escapeshellarg($chunkfilepath);
		passthru($cmd);
	} else if (is_file($datafilepath)) {
		$infofilepath = "$infopath/$file";
		$info = @parse_ini_file($infofilepath);
		$md5 = md5_file($datafilepath);
		$filesize = filesize($datafilepath);
		$timestamp = strtotime($info['date']);
		$id = base_convert(md5($timestamp . $md5), 16, 36);
		if ($info['md5'] != $md5)
			print("MD5 error	'$chunkfilepath' - '" . $info['md5'] . "' instead of '$md5'\n");
		if ($info['size'] != $filesize)
			print("Size error	'$chunkfilepath' - '" . $info['size'] . "' instead of '$filesize'\n");
		if ($info['id'] != $id)
			print("Identifier error	'$chunkfilepath' - '" . $info['id'] . "' instead of '$id'\n");
	}
}
closedir($dir);

?>
