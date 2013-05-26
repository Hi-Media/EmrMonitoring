<?php

use \GAubry\ErrorHandler\ErrorHandler;

/**
 * Bootstrap.
 *
 * @author Geoffroy AUBRY <geoffroy.aubry@hi-media.com>
 */

// Check config file
$sConfDir = realpath(dirname(__FILE__) . '/../../conf');
if ( ! file_exists($sConfDir . '/config.php')) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mConfig file missing!" . PHP_EOL
        . "    \033[0;33mcp '$sConfDir/config-dist.php' '$sConfDir/config.php' \033[0;37mand adapt it." . PHP_EOL
        . PHP_EOL;
    exit(1);
} else {
    $aConfig = include_once(dirname(__FILE__) . '/../../conf/config.php');
}

// Check composer dependencies
if ( ! file_exists($aConfig['vendor_dir'] . '/autoload.php')) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mYou must set up the project dependencies with composer." . PHP_EOL
        . "    \033[0;37mRun the following commands: \033[0;33mcomposer install"
        . "\033[0;37m or \033[0;33mphp composer.phar install\033[0;37m." . PHP_EOL
        . PHP_EOL
        . "If needed, to install \033[1;37mcomposer\033[0;37m locally: " . PHP_EOL
        . "    – \033[0;33mcurl -sS https://getcomposer.org/installer | php\033[0;37m" . PHP_EOL
        . "    – or: \033[0;33mwget --no-check-certificate -q -O- https://getcomposer.org/installer | php" . PHP_EOL
        . "\033[0;37mRead http://getcomposer.org/doc/00-intro.md#installation-nix for more information." . PHP_EOL
        . PHP_EOL;
    exit(1);
} else {
    include_once($aConfig['vendor_dir'] . '/autoload.php');
}

// Check EMR CLI
$sEMRBin = $aConfig['Himedia\EMR']['emr_elastic_mapreduce_cli'];
$sCmd = "which '$sEMRBin' 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
if (exec($sCmd) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mThe Amazon EMR Command Line Interface is missing!" . PHP_EOL
        . "    \033[0;37m1. \033[0;33msudo apt-get install ruby-full" . PHP_EOL
        . "    \033[0;37m2. \033[0;33mmkdir /usr/local/lib/elastic-mapreduce-cli" . PHP_EOL
        . "    \033[0;37m3. \033[0;33mwget http://elasticmapreduce.s3.amazonaws.com/elastic-mapreduce-ruby.zip"
        . PHP_EOL
        . "    \033[0;37m4. \033[0;33munzip -d /usr/local/lib/elastic-mapreduce-cli elastic-mapreduce-ruby.zip"
        . PHP_EOL
        . "\033[0;37mRead http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html"
        . " for more information." . PHP_EOL
        . PHP_EOL;
    exit(1);
}

set_include_path(
    $aConfig['root_dir'] . PATH_SEPARATOR .
    get_include_path()
);

// Load error/exception handler
$aEHCfg = $aConfig['GAubry\ErrorHandler'];
$GLOBALS['oErrorHandler'] = new ErrorHandler(
    $aEHCfg['display_errors'],
    $aEHCfg['error_log_path'],
    $aEHCfg['error_level'],
    $aEHCfg['auth_error_suppr_op']
);

date_default_timezone_set('Europe/Paris');
