<?php

/**
 * ANSI text formatting object.
 *
 * @author	Amaury Bouchard <amaury.bouchard@finemedia.fr>
 * @copyright	Copyright (c) 2008, Fine Media
 * @version	$Id$
 * @package	FineFS
 * @subpackage	utils
 */
class Ansi {
	/** Colors definition. */
	static public $colors = array(
		"black"		=> 0,
		"red"		=> 1,
		"green"		=> 2,
		"yellow"	=> 3,
		"blue"		=> 4,
		"magenta"	=> 5,
		"cyan"		=> 6,
		"white"		=> 7
	);

	/**
	 * Returns a bold-formatted string.
	 * @param	string	$text	Text to format.
	 * @return	string	The formatted string.
	 */
	static public function bold($text) {
		return (chr(27) . "[1m" . $text . chr(27) . "[0m");
	}
	/**
	 * Returns a faint-formatted string.
	 * @param	string	$text	Text to format.
	 * @return	string	The formatted string.
	 */
	static public function faint($text) {
		return (chr(27) . "[2m" . $text . chr(27) . "[0m");
	}
	/**
	 * Returns an underline-formatted string.
	 * @param	string	$text	Text to format.
	 * @return	string	The formatted string.
	 */
	static public function underline($text) {
		return (chr(27) . "[4m" . $text . chr(27) . "[0m");
	}
	/**
	 * Returns a video inverted-formatted string.
	 * @param	string	$text	Text to format.
	 * @return	string	The formatted string.
	 */
	static public function negative($text) {
		return (chr(27) . "[7m" . $text . chr(27) . "[0m");
	}
	/**
	 * Returns a color-formatted string.
	 * @param	string	$color	Color name (black, red, green, yellow, blue, magenta, cyan, white).
	 * @param	string	$text	Text to format.
	 * @return	string	The formatted string.
	 */
	static public function color($color, $text) {
		return (chr(27) . "[9" . self::$colors[$color] . "m" . $text . chr(27) . "[0m");
	}
	/**
	 * Returns a background colored-formatted string.
	 * @param	string	$backColor	Background color.
	 * @param	string	$color		Foreground color.
	 * @param	string	$text		Text to format.
	 * @return	string	The formatted string.
	 */
	static public function backColor($backColor, $color, $text) {
		return (chr(27) . "[4" . self::$colors[$backColor] . "m" . chr(27) . "[9" . self::$colors[$color] . "m" . $text . chr(27) . "[0m");
	}
}

?>
