<?php

/**
 * Copyright (c) 2013 Hi-Media SA
 * Copyright (c) 2013 Geoffroy Aubry <gaubry@hi-media.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 *
 * @copyright 2013 Hi-Media SA
 * @copyright 2013 Geoffroy Aubry <gaubry@hi-media.com>
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */

use \GAubry\ErrorHandler\ErrorHandler;

/**
 * Bootstrap.
 *
 * @author Geoffroy AUBRY <geoffroy.aubry@hi-media.com>
 */

// Check config file
$sConfDir = realpath(dirname(__FILE__) . '/../../conf');
if (! file_exists($sConfDir . '/config.php')) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mConfig file missing!" . PHP_EOL
        . "    \033[0;33mcp '$sConfDir/config-dist.php' '$sConfDir/config.php' \033[0;37mand adapt it." . PHP_EOL
        . PHP_EOL;
    exit(1);
} else {
    $aConfig = include_once(dirname(__FILE__) . '/../../conf/config.php');
}

// Check composer dependencies
if (! file_exists($aConfig['vendor_dir'] . '/autoload.php')) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mYou must set up the project dependencies with composer." . PHP_EOL
        . "    \033[0;37mRun the following commands: \033[0;33mcomposer install"
        . "\033[0;37m or \033[0;33mphp composer.phar install\033[0;37m." . PHP_EOL
        . PHP_EOL
        . "If needed, to install \033[1;37mcomposer\033[0;37m locally: " . PHP_EOL
        . "    – \033[0;33mcurl -sS https://getcomposer.org/installer | php\033[0;37m" . PHP_EOL
        . "    – or: \033[0;33mwget --no-check-certificate -q -O- https://getcomposer.org/installer | php"
        . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://getcomposer.org/doc/00-intro.md#installation-nix for more information." . PHP_EOL
        . PHP_EOL;
    exit(2);
} else {
    include_once($aConfig['vendor_dir'] . '/autoload.php');
}

// Check EMR CLI
$sEMRBin = $aConfig['Himedia\EMR']['emr_cli_bin'];
$sCmd = "which '$sEMRBin' 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
if (exec($sCmd) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mThe Amazon EMR Command Line Interface is missing! (search for this bin: '$sEMRBin')" . PHP_EOL
        . "    \033[0;37m1. \033[0;33msudo apt-get install ruby-full" . PHP_EOL
        . "    \033[0;37m2. \033[0;33mmkdir /usr/local/lib/elastic-mapreduce-cli" . PHP_EOL
        . "    \033[0;37m3. \033[0;33mwget http://elasticmapreduce.s3.amazonaws.com/elastic-mapreduce-ruby.zip"
        . PHP_EOL
        . "    \033[0;37m4. \033[0;33munzip -d /usr/local/lib/elastic-mapreduce-cli elastic-mapreduce-ruby.zip"
        . PHP_EOL
        . "    \033[0;37m5. Create a file named \033[0;33m/usr/local/lib/elastic-mapreduce-cli/credentials.json"
        . "\033[0;37m with at least the following lines:" . PHP_EOL
        . "          {" . PHP_EOL
        . "              \"keypair\": \"Your key pair name\"," . PHP_EOL
        . "              \"key-pair-file\": \"The path and name of your PEM/private key file\"" . PHP_EOL
        . "          }" . PHP_EOL
        . "    \033[0;37m6. If necessary, adapt \033[0;33memr_cli_bin\033[0;37m, \033[0;33maws_access_key\033[0;37m"
        . " and \033[0;33maws_secret_key\033[0;37m keys of \033[0;33m\$aConfig['Himedia\EMR']\033[0;37m"
        . " in \033[0;33mconf/config.php\033[0;37m." . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html"
        . " for more information." . PHP_EOL
        . PHP_EOL;
    exit(3);
}

// Check s3cmd
$sCmd1 = "which 's3cmd' 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
$sCmd2 = "s3cmd --dump-config 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
if (exec($sCmd1) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mCommand Line S3 client 's3cmd' not found!" . PHP_EOL
        . "    \033[0;37m1. \033[0;33msudo apt-get install s3cmd" . PHP_EOL
        . "    \033[0;37m2. \033[0;33ms3cmd --configure" . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://s3tools.org/s3cmd for more information." . PHP_EOL
        . PHP_EOL;
    exit(4);
} elseif (exec($sCmd2) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mCommand Line S3 client 's3cmd' is not configured!" . PHP_EOL
        . "    \033[0;37mPlease run \033[0;33ms3cmd --configure" . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://s3tools.org/s3cmd for more information." . PHP_EOL
        . PHP_EOL;
    exit(5);
}

// Check gnuplot
$sCmd = "which gnuplot 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
if (exec($sCmd) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mgnuplot is required!" . PHP_EOL
        . "    \033[0;37mPlease run \033[0;33msudo apt-get install gnuplot" . PHP_EOL
        . PHP_EOL;
    exit(6);
}

// Check EC2 API tools
$sEC2Dir = $aConfig['Himedia\EMR']['ec2_api_tools_dir'];
$sCmd = "which '$sEC2Dir/bin/ec2-cmd' 1>/dev/null 2>&1 && echo 'OK' || echo 'NOK'";
if (exec($sCmd) != 'OK') {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mEC2 API Tools not found! (search for this bin: '$sEC2Dir/bin/ec2-cmd')" . PHP_EOL
        . "    \033[0;37m1. \033[0;33mwget http://s3.amazonaws.com/ec2-downloads/ec2-api-tools.zip" . PHP_EOL
        . "    \033[0;37m2. \033[0;33munzip -d /usr/local/lib ec2-api-tools.zip" . PHP_EOL
        . "    \033[0;37m3. If necessary, adapt \033[0;33mec2_api_tools_dir\033[0;37m, \033[0;33maws_access_key"
        . "\033[0;37m and \033[0;33maws_secret_key\033[0;37m keys of \033[0;33m\$aConfig['Himedia\EMR']\033[0;37m"
        . " in \033[0;33mconf/config.php\033[0;37m." . PHP_EOL
        . "    \033[0;37m4. Set and export both \033[0;33mJAVA_HOME\033[0;37m and \033[0;33mEC2_HOME"
        . "\033[0;37m environment variables." . PHP_EOL
        . "       \033[0;37mFor example, include these commands in your \033[0;33m~/.bashrc\033[0;37m and reload it:"
        . PHP_EOL
        . "           \033[0;33mexport JAVA_HOME=/usr" . PHP_EOL
        . "           \033[0;33mexport EC2_HOME=/usr/local/lib/ec2-api-tools-1.6.7.2" . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/setting_up_ec2_command_linux.html"
        . " for more information." . PHP_EOL
        . PHP_EOL;
    exit(7);
}

if (empty($_SERVER['EC2_HOME']) || empty($_SERVER['JAVA_HOME'])) {
    echo "\033[1m\033[4;33m/!\\\033[0;37m "
        . "\033[0;31mMissing environment variables in order to use EC2 API Tools!" . PHP_EOL
        . "    \033[0;37mYou must set and export both \033[0;33mJAVA_HOME\033[0;37m and \033[0;33mEC2_HOME\033[0;37m."
        . PHP_EOL
        . "    \033[0;37mFor example, include these commands in your \033[0;33m~/.bashrc\033[0;37m and reload it:"
        . PHP_EOL
        . "        \033[0;33mexport JAVA_HOME=/usr" . PHP_EOL
        . "        \033[0;33mexport EC2_HOME=/usr/local/lib/ec2-api-tools-1.6.7.2" . PHP_EOL . PHP_EOL
        . "\033[0;37mRead http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/setting_up_ec2_command_linux.html"
        . " for more information." . PHP_EOL
        . PHP_EOL;
    exit(8);
}

set_include_path(
    $aConfig['root_dir'] . PATH_SEPARATOR .
    get_include_path()
);

// Load error/exception handler
$GLOBALS['oErrorHandler'] = new ErrorHandler($aConfig['GAubry\ErrorHandler']);

date_default_timezone_set('Europe/Paris');
