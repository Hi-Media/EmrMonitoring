#!/usr/bin/php
<?php


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

$aLCfg = $aConfig['GAubry\Logger'];
$oLogger = new ColoredIndentedLogger(
    $aLCfg['colors'],
    $aLCfg['tabulation'],
    $aLCfg['indent_tag'],
    $aLCfg['unindent_tag'],
    $sLogLevel
);
$oShell = new ShellAdapter($oLogger, array('ssh_connection_timeout' => 10));
$oEMRInstancePrices = new EMRInstancePrices();
$oMonitoring = new Monitoring($oLogger, $oShell, $oEMRInstancePrices, $aConfig['Himedia\EMR']);
$oRendering = new Rendering($oLogger, $oShell);

if (empty($sJobFlowID)) {
    $oRendering->displayHelp();

    $aAllJobs = $oMonitoring->getAllJobs();
    $oRendering->displayAllJobs($aAllJobs);

} else if ($bListInputFiles) {
    $aJob = $oMonitoring->getJobFlow($sJobFlowID, $sSSHTunnelPort);
    $aInputFiles = $oMonitoring->getHadoopInputFiles($sJobFlowID, $aJob);
    $oRendering->displayHadoopInputFiles($aInputFiles);

} else {
    $aJob = $oMonitoring->getJobFlow($sJobFlowID, $sSSHTunnelPort);
    $oRendering->displayJobName($aJob['Name']);
    $oRendering->displayJobGeneralStatus($aJob);
    $oRendering->displayJobInstances($aJob);
    $oRendering->displayJobSteps($aJob);

    list($sRawSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData) = $oMonitoring->getLogSummary($sJobFlowID, $aJob);
    $oRendering->displayJobSummary($aJob, $sRawSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData);

    echo "\n";
}
