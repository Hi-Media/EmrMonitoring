<?php

namespace PHP_EMR\Core;

use \GAubry\Logger\ColoredIndentedLogger;
use \Psr\Log\LogLevel;
use \GAubry\Debug\Debug;
use \GAubry\Shell\ShellAdapter;

require(dirname(__FILE__) . '/inc/bootstrap.php');

$sLogLevel = LogLevel::INFO;
foreach ($argv as $iKey => $sValue) {
    if ($sValue == '--debug' || $sValue == '-d') {
        $sLogLevel = LogLevel::DEBUG;
        array_splice($argv, $iKey, 1, array());
    }
}

$oLogger = new ColoredIndentedLogger($GLOBALS['CUI_COLORS'], EMR_LOG_TABULATION, '+++', '---', $sLogLevel);
$oShell = new ShellAdapter($oLogger, array());

if ( ! isset($argv[1])) {
    $sCmd = EMR_ELASTIC_MAPREDUCE_CLI . " --list --no-step";
    $aRawResult = $oShell->exec($sCmd);
    $oLogger->log(LogLevel::INFO, implode("\n", $aRawResult));
//     throw new \RuntimeException('Need a job flow ID in parameter');

} else {
    $sJobFlowID = $argv[1];
    $sSSHTunnelPort = (isset($argv[2]) ? (int)$argv[2] : EMR_DEFAULT_SSH_TUNNEL_PORT);
    $sCmd = EMR_ELASTIC_MAPREDUCE_CLI . " --describe $sJobFlowID";
    $aRawResult = $oShell->exec($sCmd);

    $aDesc = json_decode(implode("\n", $aRawResult), true);
    $aJob = $aDesc['JobFlows'][0];

    $sJobName = $aJob['Name'];
    $oLogger->log(LogLevel::INFO, "{C.section}Job flow name: $sJobName+++");

    displayJobStatus($aJob, $oLogger);
    displayJobInstances($aJob, $oLogger);
    displayJobSteps($aJob, $oLogger);
    scanLogs($aJob);
    echo "\n";
}

function displayJobStatus (array $aJob, ColoredIndentedLogger $oLogger)
{
    $oLogger->log(LogLevel::INFO, '{C.subsection}General+++');

    $sMasterPublicDnsName = $aJob['Instances']['MasterPublicDnsName'];
    $sMsg = str_pad('MasterPublicDnsName: ', 23, ' ')
          . ($sMasterPublicDnsName === NULL ? '–' : $sMasterPublicDnsName);
    $oLogger->log(LogLevel::INFO, $sMsg);

    $sLogUri = $aJob['LogUri'];
    $sMsg = str_pad('Log URI: ', 23, ' ') . ($sLogUri === NULL ? '–' : str_replace('s3n://', 's3://', $sLogUri));
    $oLogger->log(LogLevel::INFO, $sMsg);

    $sAvailabilityZone = $aJob['Instances']['Placement']['AvailabilityZone'];
    $sMsg = str_pad('Zone: ', 23, ' ') . ($sAvailabilityZone === NULL ? '–' : $sAvailabilityZone);
    $oLogger->log(LogLevel::INFO, $sMsg);

    $sNormalizedInstanceHours = $aJob['Instances']['NormalizedInstanceHours'];
    $sMsg = str_pad('Normalized hours: ', 23, ' ')
          . ($sNormalizedInstanceHours === NULL ? '–' : $sNormalizedInstanceHours);
    $oLogger->log(LogLevel::INFO, $sMsg);

    $aJobStatus = $aJob['ExecutionStatusDetail'];
    displayDates($aJobStatus, $oLogger);

    $oLogger->log(LogLevel::INFO, '---');
}

function displayDates (array $aJobSection, ColoredIndentedLogger $oLogger)
{
    $sStatus = $aJobSection['State'];
    $sLastStateChangeReason = $aJobSection['LastStateChangeReason'];
    displayStatus($sStatus, $sLastStateChangeReason, $oLogger);
    $sMsg = str_pad('Init/start/end date: ', 23, ' ');

    $aAllDateTypes = array('CreationDateTime', 'StartDateTime', 'EndDateTime');
    $iLastTS = 0;
    foreach ($aAllDateTypes as $sDateType) {
        if ($sDateType != 'CreationDateTime') {
            $sMsg .= '{C.section}  /  {C.info}';
        }
        $sSubMsg = ($aJobSection[$sDateType] === NULL ? '–' : date('Y-m-d H:i:s', (int)$aJobSection[$sDateType]));
        if ($iLastTS != 0 && $aJobSection[$sDateType] !== NULL) {
            $sMsg .= $sSubMsg;
            $oLastDate = new \DateTime('@' . $iLastTS);
            $oCurrentDate = new \DateTime('@' . (int)$aJobSection[$sDateType]);
            $sComment = ' (+' . $oLastDate->diff($oCurrentDate)->format('%H:%I:%S') . ')';
            $sMsg .= '{C.comment}' . $sComment;
        } else {
            $sMsg .= $sSubMsg;
        }
        $iLastTS = (int)$aJobSection[$sDateType];
    }
    $oLogger->log(LogLevel::INFO, $sMsg);
}

function displayStatus ($sStatus, $sLastStateChangeReason, ColoredIndentedLogger $oLogger)
{
    $aStatusColors = array(
        'ARRESTED' => '',
        'BOOTSTRAPPING' => 'warm_up',
        'CANCELLED' => 'warning',
        'COMPLETED' => 'ok',
        'ENDED' => 'discreet_ok',
        'FAILED' => 'error',
        'PENDING' => '',
        'PROVISIONING' => 'warm_up',
        'RESIZING' => '',
        'RUNNING' => 'running',
        'SHUTTING_DOWN' => 'warning',
        'STARTING' => 'warm_up',
        'TERMINATED' => 'ok',
        'WAITING' => 'warning',
    );

    $sMsg = str_pad('Status: ', 23, ' ')
          . ( ! empty($aStatusColors[$sStatus]) ? '{C.' . $aStatusColors[$sStatus] . '}' : '')
          . $sStatus . ( ! empty($sLastStateChangeReason) ? ", $sLastStateChangeReason" : '');
    $oLogger->log(LogLevel::INFO, $sMsg);
}

function displayJobSteps (array $aJob, ColoredIndentedLogger $oLogger)
{
    global $oShell;
    global $sSSHTunnelPort;
    $oLogger->log(LogLevel::INFO, '{C.subsection}Steps+++');

    $aJobSteps = $aJob['Steps'];
    foreach ($aJobSteps as $aJobStep) {
        $sName = $aJobStep['StepConfig']['Name'];
        $oLogger->log(LogLevel::INFO, "{C.subsubsection}$sName+++");
        $aStepStatus = $aJobStep['ExecutionStatusDetail'];
        displayDates($aStepStatus, $oLogger);

        if ($sName == 'Run Pig Script') {
            $aArgs = $aJobStep['StepConfig']['HadoopJarStep']['Args'];
            $sArgs = implode(', ', $aArgs);

            preg_match('/, -f, ([^,]+)/i', $sArgs, $aMatches);
            $sPigScript = $aMatches[1];
            $oLogger->log(LogLevel::INFO, str_pad('Script:', 23, ' ') . $sPigScript);

            preg_match('/, -p, INPUT=([^,]+)/i', $sArgs, $aMatches);
            $sPigInput = $aMatches[1];
//             $oLogger->log(LogLevel::INFO, str_pad('Input:', 23, ' ') . $sPigInput);

            preg_match('/, -p, OUTPUT=([^,]+)/i', $sArgs, $aMatches);
            $sPigOutput = $aMatches[1];
            $oLogger->log(
                LogLevel::INFO, str_pad('Input/output:', 23, ' ') . $sPigInput
                . '{C.section}  ⇒  '
                . '{C.info}' . $sPigOutput);

            if ($aStepStatus['State'] == 'RUNNING') {
                $sCmd = 'ps fax | grep -v \'ps fax\' | grep ssh'
                      . ' | grep \'' . $aJob['Instances']['MasterPublicDnsName'] . '\' | wc -l';
                $aRawResult = $oShell->exec($sCmd);
                if ($aRawResult[0] == 0) {
                    $sCmd = 'ssh -N -L ' . $sSSHTunnelPort . ':localhost:9100'
                          . ' hadoop@' . $aJob['Instances']['MasterPublicDnsName']
                          . ' -F ' . EMR_CONF_DIR . '/ssh-tunnel-config > /dev/null 2>&1 &';
                    $aRawResult = $oShell->exec($sCmd);
                    sleep(3);
                }
                displaySubJobs($oLogger);
            }
        }

        $oLogger->log(LogLevel::INFO, '---');
    }

    $oLogger->log(LogLevel::INFO, '---');
}

function displayJobInstances (array $aJob, ColoredIndentedLogger $oLogger)
{
    global $oShell;
    $oLogger->log(LogLevel::INFO, '{C.subsection}Instances+++');

    $aJobInstances = $aJob['Instances'];
    $aJobIGroups = $aJobInstances['InstanceGroups'];
    foreach ($aJobIGroups as $aJobIGroup) {
        $sName = $aJobIGroup['Name'];
        $oLogger->log(LogLevel::INFO, "{C.subsubsection}$sName+++");

        $sDesc = str_pad('Detail: ', 23, ' ') . $aJobIGroup['InstanceRole'] . ', ' . $aJobIGroup['Market'] . ', ';

        $sMsg = $aJobIGroup['InstanceRunningCount'] . '/' . $aJobIGroup['InstanceRequestCount'];
        $fRatio = 1.0 * (int)$aJobIGroup['InstanceRunningCount'] / (int)$aJobIGroup['InstanceRequestCount'];

        $oLogger->log(LogLevel::INFO, $sDesc
            . '{C.' . getColorAccordingToRatio($fRatio) . '}' . $sMsg
            . '{C.info} ' . $aJobIGroup['InstanceType']);

        displayDates($aJobIGroup, $oLogger);
        $oLogger->log(LogLevel::INFO, '---');
    }

    $oLogger->log(LogLevel::INFO, '---');
}

// function makeCamelCaseReadable ($str)
// {
//     $sFormatted = preg_replace('/(?<!^)([A-Z])/e', "' ' . strtolower('\\1')", $str);
//     return $sFormatted;
// }

function displaySubJobs (ColoredIndentedLogger $oLogger)
{
    $aSubJobKeys = array('Jobid', 'Started', 'Priority', 'User', 'Name', 'Map % Complete', 'Map Total', 'Maps Completed', 'Reduce % Complete', 'Reduce Total', 'Reduces Completed', 'Job Scheduling Information', 'Diagnostic Info');
    $dom_doc = new \DOMDocument();
    $html_file = file_get_contents('http://localhost:12345/jobtracker.jsp');
    libxml_use_internal_errors(true);
    $dom_doc->loadHTML($html_file);
    libxml_clear_errors();
    $xpath = new \DOMXPath($dom_doc);

    $trs = $xpath->query('//table/tr[th="Running Map Tasks"]/following-sibling::tr[1]');
    if ( ! empty($trs)) {
        $aValues = array();
        $tr = $trs->item(0);
        foreach($tr->childNodes as $cell) {
            $aValues[] = $cell->nodeValue;
        }
        $sMsg = str_pad('Cluster summary:', 23, ' ') . 'running maps/capacity: ';
        $sRatioMsg = $aValues[0] . '/' . $aValues[8];
        $fRatio = 1.0 * $aValues[0] / $aValues[8];
        $sMsg .= '{C.' . getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
               . '{C.info}, running reduces/capacity: ';
        $sRatioMsg =  $aValues[1] . '/' . $aValues[9];
        $fRatio = 1.0 * $aValues[1] / $aValues[9];
        $sMsg .= '{C.' . getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
        $oLogger->log(LogLevel::INFO, $sMsg);
    }

    $oLogger->log(LogLevel::INFO, '{C.section}Jobs+++');
    $trs = $xpath->query('//table[preceding-sibling::h2[@id="running_jobs"]]/tr');
    if ($trs->length > 0) {
        $aSubJobs = array();
        foreach ($trs as $tr) {
            $aSubJobValues = array();
            foreach($tr->childNodes as $cell) {
                $aSubJobValues[] = $cell->nodeValue;
            }
            if (count($aSubJobValues) == count($aSubJobKeys) && $aSubJobValues[0] != 'Jobid') {
                $aSubJob = array_combine($aSubJobKeys, array_map('trim', $aSubJobValues));
                $aSubJobs[$aSubJob['Jobid']] = $aSubJob;
            }
        }

        if (count($aSubJobs) == 0) {
            $oLogger->log(LogLevel::INFO, 'No job found…');
        } else {
            ksort($aSubJobs);
            foreach ($aSubJobs as $aSubJob) {
                $sMsg = '{C.job}' . $aSubJob['Jobid'];
                $oStartDate = \DateTime::createFromFormat('D M d H:i:s e Y', $aSubJob['Started']);
                $oStartDate->setTimezone(new \DateTimeZone(date_default_timezone_get()));
                $sMsg .= '{C.info}   start date: ' . $oStartDate->format('Y-m-d H:i:s') . ', maps completed: ';

                $sRatioMsg = $aSubJob['Maps Completed'] . '/' . $aSubJob['Map Total']
                      . ' (' . $aSubJob['Map % Complete'] . ')';
                $fRatio = substr(trim($aSubJob['Map % Complete']), 0, -1) / 100.0;
                $sMsg .= '{C.' . getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
                       . '{C.info}, reduces completed: ';

                $sRatioMsg = $aSubJob['Reduces Completed'] . '/' . $aSubJob['Reduce Total']
                      . ' (' . $aSubJob['Reduce % Complete'] . ')';
                $fRatio = substr(trim($aSubJob['Reduce % Complete']), 0, -1) / 100.0;
                $sMsg .= '{C.' . getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
                $oLogger->log(LogLevel::INFO, $sMsg);
            }
        }

    }
    $oLogger->log(LogLevel::INFO, '---');
}

function getColorAccordingToRatio ($fRatio)
{
    $aColors = array(
        0 => 'raw_error',
        1 => 'warning',
        2 => 'warm_up',
        3 => 'discreet_ok',
        4 => 'ok'
    );
    return $aColors[floor($fRatio / 0.25)];
}

function scanLogs (array $aJob)
{
    global $oLogger;
    global $oShell;
    global $sJobFlowID;

    $oLogger->log(LogLevel::INFO, '{C.subsection}Summary+++');

    $sStatus = $aJob['ExecutionStatusDetail']['State'];
    if ( ! in_array($sStatus, array('COMPLETED', 'TERMINATED', 'FAILED'))) {
        $oLogger->log(LogLevel::INFO, 'Waiting end of job flow…');

    } else {
        $sLogURI = str_replace('s3n://', 's3://', $aJob['LogUri']);
        $sLogURISteps = $sLogURI . "$sJobFlowID/steps/";
        $sCmd = "s3cmd ls '$sLogURISteps' | grep DIR | sed 's/ *DIR *//'";
        $aRawResult = $oShell->exec($sCmd);

        if (count($aRawResult) == 0) {
            $oLogger->log(LogLevel::INFO, 'No log found!');
        } else {
            foreach ($aRawResult as $sStepURI) {
                $sTmpFilename = '/tmp' . '/php-emr_' . md5(time().rand());
                $sCmd = "s3cmd get '{$sStepURI}stderr' '$sTmpFilename'";
                $aRawResult = $oShell->exec($sCmd);
                $sContent = file_get_contents($sTmpFilename);
                if (preg_match('/^(Job Stats \(time in seconds\):.*+)/sm', $sContent, $aMatches) === 1) {
                    print_r($aMatches[1]);
                } else if (preg_match_all('/^([^[]+\s\[[^]]+\]\sERROR\s.*?)(?:^[^[]+\s\[[^]]+\]|^Command exiting with ret \'255\'$)/sm', $sContent, $aMatches) != 0) {
                    foreach ($aMatches[1] as $sRow) {
                        $oLogger->log(LogLevel::ERROR, trim($sRow));
                    }
                }
                unlink($sTmpFilename);
            }
        }
    }

    $oLogger->log(LogLevel::INFO, '---');
}

/*

Job flow JSON:

Array
(
    [JobFlows] => Array
        (
            [0] => Array
                (
                    [VisibleToAllUsers] =>
                    [SupportedProducts] => Array
                        (
                        )

                    [ExecutionStatusDetail] => Array
                        (
                            [ReadyDateTime] => 1362497722
                            [State] => COMPLETED
                            [CreationDateTime] => 1362497376
                            [StartDateTime] => 1362497722
                            [LastStateChangeReason] => Steps completed
                            [EndDateTime] => 1362498948
                        )

                    [AmiVersion] => 2.3.3
                    [LogUri] => s3n://gaubry-test/hadoop-log/
                    [JobFlowId] => j-IU0IBC7LEGXZ
                    [JobFlowRole] =>
                    [BootstrapActions] => Array
                        (
                        )

                    [Instances] => Array
                        (
                            [MasterInstanceType] => m1.medium
                            [TerminationProtected] =>
                            [Placement] => Array
                                (
                                    [AvailabilityZone] => eu-west-1a
                                )

                            [HadoopVersion] => 1.0.3
                            [MasterPublicDnsName] => ec2-54-228-67-3.eu-west-1.compute.amazonaws.com
                            [SlaveInstanceType] => m1.medium
                            [Ec2KeyName] => gaubry
                            [MasterInstanceId] => i-f0a133ba
                            [Ec2SubnetId] =>
                            [InstanceCount] => 5
                            [KeepJobFlowAliveWhenNoSteps] =>
                            [InstanceGroups] => Array
                                (
                                    [0] => Array
                                        (
                                            [ReadyDateTime] => 1362497644
                                            [InstanceRequestCount] => 1
                                            [State] => ENDED
                                            [CreationDateTime] => 1362497376
                                            [StartDateTime] => 1362497579
                                            [InstanceRole] => MASTER
                                            [InstanceRunningCount] => 0
                                            [InstanceGroupId] => ig-2RYF3JW2X9F0U
                                            [Market] => ON_DEMAND
                                            [BidPrice] =>
                                            [LastStateChangeReason] => Job flow terminated
                                            [InstanceType] => m1.medium
                                            [EndDateTime] => 1362498948
                                            [Name] => Master Instance Group
                                            [LaunchGroup] =>
                                        )

                                    [1] => Array
                                        (
                                            [ReadyDateTime] => 1362498258
                                            [InstanceRequestCount] => 4
                                            [State] => ENDED
                                            [CreationDateTime] => 1362497376
                                            [StartDateTime] => 1362498258
                                            [InstanceRole] => CORE
                                            [InstanceRunningCount] => 0
                                            [InstanceGroupId] => ig-JG8QVI9W3VPU
                                            [Market] => ON_DEMAND
                                            [BidPrice] =>
                                            [LastStateChangeReason] => Job flow terminated
                                            [InstanceType] => m1.medium
                                            [EndDateTime] => 1362498948
                                            [Name] => Core Instance Group
                                            [LaunchGroup] =>
                                        )

                                )

                            [NormalizedInstanceHours] => 10
                        )

                    [Name] => gaubry-test
                    [Steps] => Array
                        (
                            [0] => Array
                                (
                                    [ExecutionStatusDetail] => Array
                                        (
                                            [State] => COMPLETED
                                            [CreationDateTime] => 1362497376
                                            [StartDateTime] => 1362497721
                                            [LastStateChangeReason] =>
                                            [EndDateTime] => 1362497737
                                        )

                                    [StepConfig] => Array
                                        (
                                            [ActionOnFailure] => TERMINATE_JOB_FLOW
                                            [HadoopJarStep] => Array
                                                (
                                                    [Args] => Array
                                                        (
                                                            [0] => s3://eu-west-1.elasticmapreduce/libs/state-pusher/0.1/fetch
                                                        )

                                                    [Jar] => s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar
                                                    [Properties] => Array
                                                        (
                                                        )

                                                    [MainClass] =>
                                                )

                                            [Name] => Setup Hadoop Debugging
                                        )

                                )

                            [1] => Array
                                (
                                    [ExecutionStatusDetail] => Array
                                        (
                                            [State] => COMPLETED
                                            [CreationDateTime] => 1362497376
                                            [StartDateTime] => 1362497737
                                            [LastStateChangeReason] =>
                                            [EndDateTime] => 1362497768
                                        )

                                    [StepConfig] => Array
                                        (
                                            [ActionOnFailure] => TERMINATE_JOB_FLOW
                                            [HadoopJarStep] => Array
                                                (
                                                    [Args] => Array
                                                        (
                                                            [0] => s3://eu-west-1.elasticmapreduce/libs/pig/pig-script
                                                            [1] => --base-path
                                                            [2] => s3://eu-west-1.elasticmapreduce/libs/pig/
                                                            [3] => --install-pig
                                                            [4] => --pig-versions
                                                            [5] => latest
                                                        )

                                                    [Jar] => s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar
                                                    [Properties] => Array
                                                        (
                                                        )

                                                    [MainClass] =>
                                                )

                                            [Name] => Setup Pig
                                        )

                                )

                            [2] => Array
                                (
                                    [ExecutionStatusDetail] => Array
                                        (
                                            [State] => COMPLETED
                                            [CreationDateTime] => 1362497376
                                            [StartDateTime] => 1362497768
                                            [LastStateChangeReason] =>
                                            [EndDateTime] => 1362498883
                                        )

                                    [StepConfig] => Array
                                        (
                                            [ActionOnFailure] => CANCEL_AND_WAIT
                                            [HadoopJarStep] => Array
                                                (
                                                    [Args] => Array
                                                        (
                                                            [0] => s3://eu-west-1.elasticmapreduce/libs/pig/pig-script
                                                            [1] => --base-path
                                                            [2] => s3://eu-west-1.elasticmapreduce/libs/pig/
                                                            [3] => --pig-versions
                                                            [4] => latest
                                                            [5] => --run-pig-script
                                                            [6] => --args
                                                            [7] => -f
                                                            [8] => s3://gaubry-test/input/test.pig
                                                            [9] => -p
                                                            [10] => INPUT=s3://gaubry-test/input/05b/17-21.log.lzo
                                                            [11] => -p
                                                            [12] => OUTPUT=s3://gaubry-test/output/4
                                                        )

                                                    [Jar] => s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar
                                                    [Properties] => Array
                                                        (
                                                        )

                                                    [MainClass] =>
                                                )

                                            [Name] => Run Pig Script
                                        )
                                )
                        )
                )
        )
)

 */