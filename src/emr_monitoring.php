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

// php vendor/bin/phpcs --standard=PSR2 src/
// php vendor/bin/phpmd src/ text codesize,design,unusedcode,naming,controversial

use Himedia\EMR\EMRInstancePrices;
use Himedia\EMR\Monitoring;
use GAubry\Logger\ColoredIndentedLogger;
use Psr\Log\LogLevel;
use GAubry\Debug\Debug;
use GAubry\Shell\ShellAdapter;
use Himedia\EMR\Rendering;

require(dirname(__FILE__) . '/inc/bootstrap.php');

// Parameters
$bListInputFiles = false;
$sLogLevel = LogLevel::INFO;
foreach ($argv as $iKey => $sValue) {
    if ($sValue == '--debug' || $sValue == '-d') {
        $sLogLevel = LogLevel::DEBUG;
        array_splice($argv, $iKey, 1, array());
    }
    if ($sValue == '--list-input-files') {
        $bListInputFiles = true;
        array_splice($argv, $iKey, 1, array());
    }
}
$sJobFlowID = (isset($argv[1]) ? $argv[1] : '');
$sSSHTunnelPort = (isset($argv[2]) ? (int)$argv[2] : $aConfig['Himedia\EMR']['default_ssh_tunnel_port']);

$aConfig['GAubry\Logger']['min_message_level'] = $sLogLevel;
$oLogger = new ColoredIndentedLogger($aConfig['GAubry\Logger']);
$oEMRInstancePrices = new EMRInstancePrices();
$oMonitoring = new Monitoring($oLogger, $oEMRInstancePrices, $aConfig['Himedia\EMR']);
$oRendering = new Rendering($oLogger);

if (empty($sJobFlowID)) {
    $oRendering->displayHelp();

    $aAllJobs = $oMonitoring->getAllJobs();
    $oRendering->displayAllJobs($aAllJobs);

} elseif ($bListInputFiles) {
    $aJob = $oMonitoring->getJobFlow($sJobFlowID, $sSSHTunnelPort);
    $aInputFiles = $oMonitoring->getHadoopInputFiles($sJobFlowID, $aJob);
    $oRendering->displayHadoopInputFiles($aInputFiles);

} else {
    $aJob = $oMonitoring->getJobFlow($sJobFlowID, $sSSHTunnelPort);
    $oRendering->displayJobName($aJob['Name']);
    $oRendering->displayJobGeneralStatus($aJob);
    $oRendering->displayJobInstances($aJob);
    $oRendering->displayJobSteps($aJob);

    list($sRawSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData)
        = $oMonitoring->getLogSummary($sJobFlowID, $aJob);
    $oRendering->displayJobSummary(
        $aJob,
        $sRawSummary,
        $aErrorMsg,
        $aS3LogSteps,
        $iMaxTs,
        $iMaxNbTasks,
        $sGnuplotData,
        $GLOBALS['aConfig']['inc_dir'] . '/plot.script'
    );

    echo "\n";
}
