<?php

if (!class_exists("IOException")) {

/**
 * Exception for IO error management.
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2007, FineMedia
 * @package	FineFS
 * @subpackage	utils
 * @version	$Id$
 */
class IOException extends Exception {
	/** Fundamental error constant. */
	const FUNDAMENTAL = 0;
	/** File not found error constant. */
	const NOT_FOUND = 1;
	/** Reading error constant. */
	const UNREADABLE = 2;
	/** Writing error constant. */
	const UNWRITABLE = 3;
	/** Format error constant. */
	const BAD_FORMAT = 4;
	/** Lock error constant. */
	const UNLOCKABLE = 5;
}

} // class_exists

?>
