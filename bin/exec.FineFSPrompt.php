#!/usr/bin/php
<?php

/**
 * FineFS minimal shell.
 * Command-line utility program used to manage the local FineFS cluster's data.
 *
 * Several commands can be used:
 * ls		Shows the files and sub-directories stored under the current working
 *		directory or under a given path.
 * cd		Change the current working directory.
 * info		Shows information about a given file.
 * more		Display the content of a given file.
 * rm		Remove a given file.
 * get		Copy a given file of the cluster to a local path.
 * help		Shows the program's help.
 * exit		Quit the program.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2009, FineMedia
 * @package	FineFS
 * @subpackage	bin
 * @version	$Id$
 */

// includes
$fineFSroot = dirname(__FILE__) . "/..";
chdir($fineFSroot);
require_once("$fineFSroot/lib/php/class.FineFS.php");
require_once("$fineFSroot/lib/php/utils/class.Ansi.php");

// current path on cluster's data
$path = "";

// FineFS object creation
$finefs = FineFS::factory();

// list of allowed commands
$commands = array(
	"help",
	"ls",
	"cd",
	"info",
	"more",
	"rm",
	"exit",
	"get",
	"put",
	"ln"
);

// main loop
for ($mustLoop = true; $mustLoop; )
	processCommandLine($finefs, $path, $mustLoop);

/**
 * Command line management. Display prompt, read the new command and process it.
 * @param	FineFS	$finefs		FineFS object.
 * @param	string	$path		Current path on cluster's data.
 * @param	bool	$mustLoop	True if the main loop must continue.
 */
function processCommandLine(&$finefs, &$path, &$mustLoop) {
	global $commands;

	readline_completion_function(defaultReadlineCompletion);
	$s = readline("/$path> ");
	readline_add_history($s);
	$elems = explode(" ", $s);
	$command = strtolower(array_shift($elems));
	if (empty($command) || !in_array($command, $commands)) {
		commandHelp($finefs, $path, $mustLoop, null);
		return;
	}
	$command = "command" . ucfirst($command);
	$command($finefs, $path, $mustLoop, $elems);
	$path = trim($path, "/");
}
/** ln command. */
function commandLn(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[0]);
	$target = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$target = dirname($path);
		else
			$target .= "/$element";
	}
	$elements = explode("/", $params[1]);
	$file = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$file = dirname($path);
		else
			$file .= "/$element";
	}
	try {
		$finefs->link($file, $target);
	} catch (Exception $e) {
		print(Ansi::color('red', "Link error.") . "\n");
		return;
	}
	print(Ansi::bold("File '$file' successfully linked to '$target'.\n"));
}
/** put command. */
function commandPut(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[1]);
	$npath = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$npath = dirname($path);
		else
			$npath .= "/$element";
	}
	try {
		$finefs->putFile($npath, $params[0]);
	} catch (Exception $e) {
		print(Ansi::color('red', "Copy error.") . "\n");
		return;
	}
	print(Ansi::bold("File '" . $params[0] . "' successfully copied to '$npath'.\n"));
}
/** get command. */
function commandGet(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[0]);
	$npath = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$npath = dirname($path);
		else
			$npath .= "/$element";
	}
	if (empty($params[1])) {
		print(Ansi::color('red', "No destination.") . "\n");
		return;
	}
	// get file information
	try {
		$info = $finefs->getInfo($npath);
	} catch (Exception $e) {
		print(Ansi::color('red', "File not found.") . "\n");
		return;
	}
	if ($info['mime'] == "inode/directory") {
		print(Ansi::color('red', "File not found.") . "\n");
		return;
	}
	// copy the file locally
	try {
		$finefs->getFile($npath, $params[1]);
	} catch (Exception $e) {
		print(Ansi::color('red', "Copy error.") . "\n");
		return;
	}
	print(Ansi::bold("File '$npath' successfully copied to '" . $params[1] . "'.\n"));
}
/** rm command. */
function commandRm(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[0]);
	$npath = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$npath = dirname($path);
		else
			$npath .= "/$element";
	}
	try {
		$info = $finefs->getInfo($npath);
	} catch (Exception $e) {
		print(Ansi::color('red', "File not found.") . "\n");
		return;
	}
	if ($info['mime'] == "inode/directory") {
		print(Ansi::color('red', "Can't remove directories.") . "\n");
		return;
	}
	// remove the file
	try {
		$finefs->remove($npath);
	} catch (Exception $e) {
		print(Ansi::color('red', "Deletion error.") . "\n");
		return;
	}
	print(Ansi::bold("File '$npath' removed.\n"));
}
/** more command. */
function commandMore(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[0]);
	$npath = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$npath = dirname($path);
		else
			$npath .= "/$element";
	}
	// get file data
	try {
		$info = $finefs->getData($npath);
	} catch (Exception $e) {
		print(Ansi::color('red', "File not found.") . "\n");
		return;
	}
	// show file name, meta-data and content
	print(Ansi::bold(Ansi::color('magenta', $npath)) . "\n");
	foreach ($info as $name => $value) {
		if ($name == 'data')
			continue;
		print(Ansi::color('blue', $name) . "\t" . Ansi::color('yellow', $value) . "\n");
	}
	print(Ansi::color('green', $info['data']) . "\n");
}
/** info command. */
function commandInfo(&$finefs, &$path, &$mustLoop, $params) {
	$elements = explode("/", $params[0]);
	$npath = $path;
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$npath = dirname($path);
		else
			$npath .= "/$element";
	}
	// get file meta-data
	try {
		$info = $finefs->getInfo($npath);
	} catch (Exception $e) {
		print(Ansi::color('red', "File not found.") . "\n");
		return;
	}
	// show file name and meta-data
	print(Ansi::bold(Ansi::color('magenta', $npath)) . "\n");
	foreach ($info as $name => $value)
		print(Ansi::color('blue', $name) . "\t" . Ansi::color('yellow', $value) . "\n");
}
/** cd command. */
function commandCd(&$finefs, &$path, &$mustLoop, $params) {
	$oldpath = $path;
	$elements = explode("/", $params[0]);
	foreach ($elements as $element) {
		$element = trim($element);
		if (empty($element) || $element == ".")
			continue;
		if ($element == "..")
			$path = dirname($path);
		else
			$path .= "/$element";
	}
	$path = "/$path/";
	while (strpos($path, "/../") !== false)
		$path = str_replace("/../", "", $path);
	while (strpos($path, "/./") !== false)
		$path = str_replace("/./", "", $path);
	while (strpos($path, "//") !== false)
		$path = str_replace("//", "/", $path);
	$path = trim($path, "/");
}
/** ls command. */
function commandLs(&$finefs, &$path, &$mustLoop, $params) {
	$npath = empty($params[0]) ? $path : ($path . "/" . $params[0]);
	$npath = trim($npath, "/");
	// get file list
	try {
		$list = $finefs->getList($npath . "/*");
	} catch (Exception $e) {
		print(Ansi::color('red', "Path not found.") . "\n");
		return;
	}
	// show files
	foreach ($list as $filename => $meta) {
		$name = $filename;
		if (substr($filename, 0, strlen($npath)) == $npath)
			$name = substr($filename, strlen($npath));
		$name = trim($name, "/");
		if ($meta['mime'] == "inode/directory")
			print(Ansi::color('cyan', $name . "/") . "\n");
		else
			print(Ansi::color('blue', $name) . "\t" . Ansi::faint($meta['size']) . "\n");
	}
}
/* help command. */
function commandHelp(&$finefs, &$path, &$mustLoop, $params) {
	print(Ansi::underline("Options:") . "\n");
	print("\n");
	print(Ansi::bold("exit") . "\n");
	print("\tQuits this program.\n\n");
	print(Ansi::bold("help") . "\n");
	print("\tShows this help.\n\n");
	print(Ansi::bold("ls") . " " . Ansi::faint("distant_path") . "\n");
	print("\tShows list of files and subdirectories.\n\n");
	print(Ansi::bold("cd") . " " . Ansi::faint("distant_path") . "\n");
	print("\tMove to another directory.\n\n");
	print(Ansi::bold("info") . " " . Ansi::faint("distant_path") . "\n");
	print("\tShows information about a file.\n\n");
	print(Ansi::bold("more") . " " . Ansi::faint("distant_path") . "\n");
	print("\tDisplay the content of a file.\n\n");
	print(Ansi::bold("rm") . " " . Ansi::faint("distant_path") . "\n");
	print("\tRemove a file.\n\n");
	print(Ansi::bold("get") . " " . Ansi::faint("distant_path") . " " . Ansi::faint("local_path") . "\n");
	print("\tCopy a distant file on the local filesystem.\n\n");
	print(Ansi::bold("put") . " " . Ansi::faint("local_path") . " " . Ansi::faint("distant_path") . "\n");
	print("\tCopy a local file on the cluster.\n\n");
	print(Ansi::bold("ln") . " " . Ansi::faint("distant_path_target") . " " . Ansi::faint("distant_path_file") . "\n");
	print("\tCreate a link from one file of the cluster to another.\n\n");
}
/** exit command. */
function commandExit(&$finefs, &$path, &$mustLoop, $params) {
	$mustLoop = false;
}
/**
 * Default callback for readline completion.
 * @param	string	$name	Text to complete.
 * @return	array	List of possible completions.
 */
function defaultReadlineCompletion($name) {
	global $commands;

	if (empty($name))
		return ($commands);
	$result = array();
	foreach ($commands as $command) {
		if (substr($command, 0, strlen($name)) == $name)
			$result[] = $command;
	}
	return ($result);
}

?>
