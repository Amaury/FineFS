<?php

if (!class_exists("ApplicationException")) {

/**
 * Exception for application error management.
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2007, FineMedia
 * @package	FineFS
 * @subpackage	utils
 * @version	$Id$
 */
class ApplicationException extends Exception {
	/** Unknown error constant. */
	const UNKNOWN = -1;
	/** API error constant. */
	const API = 0;
	/** System error constant. */
	const SYSTEM = 1;
	/** Authentication error constant. */
	const AUTHENTICATION = 2;
}

} // class_exists

?>
