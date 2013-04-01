<?php

use \GAubry\ErrorHandler\ErrorHandler;

/**
 * Bootstrap.
 *
 * @author Geoffroy AUBRY <geoffroy.aubry@hi-media.com>
 */

include_once(dirname(__FILE__) . '/../../conf/config.php');

if ( ! file_exists(EMR_ROOT_DIR . '/vendor/autoload.php')) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "You must set up the project dependencies, run the following commands:" . PHP_EOL
        . "    \033[0;33mcomposer install\033[0;37m or \033[0;33mphp composer.phar install\033[0;37m." . PHP_EOL
        . PHP_EOL
        . "If needed, to install \033[1;37mcomposer\033[0;37m locally: "
        . "\033[0;37m\033[0;33mcurl -sS https://getcomposer.org/installer | php\033[0;37m" . PHP_EOL
        . "Or check http://getcomposer.org/doc/00-intro.md#installation-nix for more information." . PHP_EOL
        . PHP_EOL;
    exit(1);
}

include_once(EMR_ROOT_DIR . '/vendor/autoload.php');

set_include_path(
    EMR_ROOT_DIR . PATH_SEPARATOR .
    get_include_path()
);

// ClassLoader::register('', EMR_LIB_DIR);
// ClassLoader::register('PHP_EMR', EMR_CLASS_DIR);

$GLOBALS['oErrorHandler'] = new ErrorHandler(
    EMR_DISPLAY_ERRORS,
    EMR_ERROR_LOG_PATH,
    EMR_ERROR_LEVEL,
    EMR_AUTH_ERROR_SUPPR_OP
);

date_default_timezone_set('Europe/Paris');