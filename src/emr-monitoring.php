#!/usr/bin/php
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

use GAubry\Logger\ColoredIndentedLogger;
use Himedia\EMR\Controller;
use Himedia\EMR\EMRInstancePrices;
use Himedia\EMR\Monitoring;
use Himedia\EMR\Rendering;
use Psr\Log\LogLevel;
use Ulrichsg\Getopt;

require(dirname(__FILE__) . '/inc/bootstrap.php');



// Extract command line parameters
$oGetopt = new Getopt(array(
    array('h', 'help', Getopt::NO_ARGUMENT),
    array('d', 'debug', Getopt::NO_ARGUMENT),
    array('l', 'list-all-jobflows', Getopt::NO_ARGUMENT),
    array('j', 'jobflow-id', Getopt::REQUIRED_ARGUMENT),
    array(null, 'list-input-files', Getopt::NO_ARGUMENT),
    array('p', 'ssh-tunnel-port', Getopt::REQUIRED_ARGUMENT)
));
try {
    $oGetopt->parse();
    $sError = '';
} catch (UnexpectedValueException $oException) {
    $sError = $oException->getMessage();
}
$aParameters = $oGetopt->getOptions() + array('error' => $sError);



// Init.
$aConfig['GAubry\Logger']['min_message_level']
    = ($oGetopt->getOption('debug') !== null ? LogLevel::DEBUG : LogLevel::INFO);
$oLogger = new ColoredIndentedLogger($aConfig['GAubry\Logger']);
$oMonitoring = new Monitoring($oLogger, new EMRInstancePrices(), $aConfig['Himedia\EMR']);
$oRendering = new Rendering($oLogger);
$oController = new Controller($aConfig, $oMonitoring, $oRendering);



// Run
$oController->run($aParameters);
