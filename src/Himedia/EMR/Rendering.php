<?php

namespace Himedia\EMR;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use GAubry\Helpers\Helpers;

class Rendering
{
    private $_oLogger;

    public function __construct (LoggerInterface $oLogger)
    {
        $this->_oLogger = $oLogger;
    }

    public function displayHelp ()
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.section}Help+++');
        $this->_oLogger->log(LogLevel::INFO, '{C.help_cmd}emr_monitoring.php+++');
        $this->_oLogger->log(LogLevel::INFO, 'Display this help and list all job flows in the last 2 weeks.---');
        $this->_oLogger->log(LogLevel::INFO, '{C.help_cmd}emr_monitoring.php {C.help_param}<jobflowid>+++');
        $this->_oLogger->log(LogLevel::INFO, 'Display statistics on any {C.help_param}<jobflowid>{C.info}, finished or in progress.');
        $this->_oLogger->log(LogLevel::INFO, 'Add {C.help_param}-d{C.info} or {C.help_param}--debug{C.info} to enable debug mode.');
        $this->_oLogger->log(LogLevel::INFO, '⇒ to monitor a jobflow in real-time: {C.help_cmd}watch -n10 --color emr_monitoring.php {C.help_param}<jobflowid>---');
        $this->_oLogger->log(LogLevel::INFO, '{C.help_cmd}emr_monitoring.php {C.help_param}<jobflowid>{C.help_cmd} --list-input-files+++');
        $this->_oLogger->log(LogLevel::INFO, 'List all S3 input files really loaded by Hadoop instance of the completed {C.help_param}<jobflowid>{C.info}.---');
        $this->_oLogger->log(LogLevel::INFO, ' ---');
    }

    /**
     *
     *
     * @param array $aAllJobs Array of string "job-id    status    master-node    name".
     */
    public function displayAllJobs (array $aAllJobs)
    {
        $this->_oLogger->log(LogLevel::INFO, "{C.section}All job flows+++");
        $this->_oLogger->log(LogLevel::INFO, implode("\n", $aAllJobs) . '---');
    }

    public function displayJobName ($sJobName)
    {
        $this->_oLogger->log(LogLevel::INFO, "{C.section}Job flow name: $sJobName+++");
    }

    public function displayJobGeneralStatus (array $aJob)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsection}General+++');

        $sMasterPublicDnsName = $aJob['Instances']['MasterPublicDnsName'];
        $sMsg = str_pad('MasterPublicDnsName: ', 23, ' ')
              . ($sMasterPublicDnsName === null ? '–' : $sMasterPublicDnsName);
        $this->_oLogger->log(LogLevel::INFO, $sMsg);

        $sLogUri = $aJob['LogUri'];
        $sMsg = str_pad('Log URI: ', 23, ' ') . ($sLogUri === null ? '–' : str_replace('s3n://', 's3://', $sLogUri));
        $this->_oLogger->log(LogLevel::INFO, $sMsg);

        $sAvailabilityZone = $aJob['Instances']['Placement']['AvailabilityZone'];
        $sMsg = str_pad('Zone: ', 23, ' ') . ($sAvailabilityZone === null ? '–' : $sAvailabilityZone);
        $this->_oLogger->log(LogLevel::INFO, $sMsg);

        $sNormalizedInstanceHours = $aJob['Instances']['NormalizedInstanceHours'];
        $fMaxTotalPrice = $aJob['Instances']['MaxTotalPrice'];
        $sMsg = str_pad('Norm. hours, price: ', 23, ' ')
              . (empty($sNormalizedInstanceHours) ? '–' : $sNormalizedInstanceHours .'h')
              . ', {C.price}' . ($fMaxTotalPrice == 0 ? '–' : '≤ $' . $fMaxTotalPrice);
        $this->_oLogger->log(LogLevel::INFO, $sMsg);
        $this->_displayStatusAndDates($aJob['ExecutionStatusDetail']);

        $this->_oLogger->log(LogLevel::INFO, '---');
    }

    private function _displayStatusAndDates (array $aJobSection)
    {
        $sStatus = $aJobSection['State'];
        $sLastStateChangeReason = $aJobSection['LastStateChangeReason'];
        $sColoredStatus = $this->_getColoredStatus($sStatus, $sLastStateChangeReason);
        $this->_oLogger->log(LogLevel::INFO, $sColoredStatus);
        $sMsg = str_pad('Init/start/end date: ', 23, ' ');

        $aAllDateTypes = array('CreationDateTime', 'StartDateTime', 'EndDateTime');
        foreach ($aAllDateTypes as $sDateType) {
            if ($sDateType != 'CreationDateTime') {
                $sMsg .= '{C.section}  /  {C.info}';
            }
            $sMsg .= ($aJobSection[$sDateType] === null ? '–' : date('Y-m-d H:i:s', (int)$aJobSection[$sDateType]));
            if ( ! empty($aJobSection["ElapsedTimeTo$sDateType"])) {
                $sMsg .= ' {C.comment}('
                       . ($aJobSection[$sDateType] === null ? '≈ ' : '')
                       . '+' . gmdate('H:i:s', $aJobSection["ElapsedTimeTo$sDateType"]) . ')';
            }
        }

        $this->_oLogger->log(LogLevel::INFO, $sMsg);
    }

    private function _getColoredStatus ($sStatus, $sLastStateChangeReason)
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
            'RESIZING' => 'warm_up',
            'RUNNING' => 'running',
            'SHUTTING_DOWN' => 'warning',
            'STARTING' => 'warm_up',
            'TERMINATED' => 'ok',
            'WAITING' => 'warning',
        );

        $sMsg = str_pad('Status: ', 23, ' ')
              . ( ! empty($aStatusColors[$sStatus]) ? '{C.' . $aStatusColors[$sStatus] . '}' : '')
              . $sStatus . ( ! empty($sLastStateChangeReason) ? ", $sLastStateChangeReason" : '');
        return $sMsg;
    }

    public function displayJobInstances (array $aJob)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsection}Instances+++');

        foreach ($aJob['Instances']['InstanceGroups'] as $aJobIGroup) {
            $sName = $aJobIGroup['Name'];
            $this->_oLogger->log(LogLevel::INFO, "{C.subsubsection}$sName+++");

            $sMarket = $aJobIGroup['Market'];
            if (empty($aJobIGroup['AskPrice'])) {
                $sAskPrice = '–';
            } else {
                $sAskPrice = '$' . $aJobIGroup['AskPrice'];
                if ($aJobIGroup['AskPrice'] > $aJobIGroup['BidPrice']) {
                    $sAskPrice = "{C.raw_error}$sAskPrice{C.price}";
                } else if ($aJobIGroup['AskPrice'] == $aJobIGroup['BidPrice']) {
                    $sAskPrice = "{C.warning}$sAskPrice{C.price}";
                }
            }
            if ($sMarket == 'SPOT') {
                $fPrice = 'bid:$' . $aJobIGroup['BidPrice'] . ' ask:' . $sAskPrice;
            } else if ( ! empty($aJobIGroup['OnDemandPrice'])) {
                $fPrice = '$' . $aJobIGroup['OnDemandPrice'];
            } else {
                $fPrice = '';
            }

            $sMsg = $aJobIGroup['InstanceRunningCount'] . '/' . $aJobIGroup['InstanceRequestCount'];
            $fRatio = 1.0 * (int)$aJobIGroup['InstanceRunningCount'] / (int)$aJobIGroup['InstanceRequestCount'];
            $sDesc = str_pad('Detail: ', 23, ' ') . $aJobIGroup['InstanceRole'] . ', ' . $sMarket . ', ';
            $this->_oLogger->log(LogLevel::INFO, $sDesc
                . '{C.' . $this->_getColorAccordingToRatio($fRatio) . '}' . $sMsg
                . '{C.info} ' . $aJobIGroup['InstanceType']
                . ', {C.price}' . (empty($fPrice) ? '–' : $fPrice . ' /h/instance'));
            if (isset($aJobIGroup['AskPriceError']) && $aJobIGroup['AskPriceError'] instanceof \RuntimeException) {
                $sMsg = 'Error when fetching spot instance pricing!'
                      . "\n{C.raw_error}"
                      . str_replace("\n", "\n{C.raw_error}", $aJobIGroup['AskPriceError']);
                $this->_oLogger->log(LogLevel::ERROR, $sMsg);
            }

            $this->_displayStatusAndDates($aJobIGroup);
            $this->_oLogger->log(LogLevel::INFO, '---');
        }

        $this->_oLogger->log(LogLevel::INFO, '---');
    }

    private function _getColorAccordingToRatio ($fRatio)
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

    public function displayJobSteps (array $aJob)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsection}Steps+++');

        $aJobSteps = $aJob['Steps'];
        foreach ($aJobSteps as $aJobStep) {
            $sName = $aJobStep['StepConfig']['Name'];
            $this->_oLogger->log(LogLevel::INFO, "{C.subsubsection}$sName+++");
            $aStepStatus = $aJobStep['ExecutionStatusDetail'];
            $this->_displayStatusAndDates($aStepStatus);

            if ($sName == 'Run Pig Script') {
                $this->_oLogger->log(LogLevel::INFO, str_pad('Script:', 23, ' ') . $aJobStep['PigScript']);

                $this->_oLogger->log(
                    LogLevel::INFO,
                    str_pad('Input/output (size):', 23, ' ')
                        . $aJobStep['PigInput'] . $aJobStep['PigInputSize'] . '{C.section}  ⇒  '
                        . '{C.info}' . $aJobStep['PigOutput'] . $aJobStep['PigOutputSize']
                );

                if ($aStepStatus['State'] == 'RUNNING') {
                    $this->_displaySubJobs($aJobStep);
                }
            }

            $this->_oLogger->log(LogLevel::INFO, '---');
        }

        $this->_oLogger->log(LogLevel::INFO, '---');
    }

    private function _displaySubJobs (array $aJobStep)
    {
        if (count($aJobStep['ClusterSummary']) > 0) {
            $aValues = $aJobStep['ClusterSummary'];
            $sMsg = str_pad('Cluster summary:', 23, ' ') . 'running maps/capacity: ';
            $sRatioMsg = $aValues[0] . '/' . $aValues[8];
            $fRatio = 1.0 * $aValues[0] / $aValues[8];
            $sMsg .= '{C.' . $this->_getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
                   . '{C.info}, running reduces/capacity: ';
            $sRatioMsg =  $aValues[1] . '/' . $aValues[9];
            $fRatio = 1.0 * $aValues[1] / $aValues[9];
            $sMsg .= '{C.' . $this->_getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
            $this->_oLogger->log(LogLevel::INFO, $sMsg);
        }

        $this->_oLogger->log(LogLevel::INFO, '{C.section}Jobs+++');
        $aSubJobsSummary = $aJobStep['SubJobsSummary'];
        if ( ! empty($aJobStep['Error'])) {
            $this->_oLogger->log(LogLevel::ERROR, $aJobStep['Error']);
        } else if (count($aSubJobsSummary) == 0) {
            $this->_oLogger->log(LogLevel::INFO, 'No job found…');
        } else {
            ksort($aSubJobsSummary);
            foreach ($aSubJobsSummary as $aSubJob) {
                $sMsg = '{C.job}' . $aSubJob['Jobid'];
                $oStartDate = \DateTime::createFromFormat('D M d H:i:s e Y', $aSubJob['Started']);
                $oStartDate->setTimezone(new \DateTimeZone(date_default_timezone_get()));
                $sMsg .= '{C.info}   start date: ' . $oStartDate->format('Y-m-d H:i:s') . ', maps completed: ';

                $sRatioMsg = $aSubJob['Maps Completed'] . '/' . $aSubJob['Map Total']
                      . ' (' . $aSubJob['Map % Complete'] . ')';
                $fRatio = substr(trim($aSubJob['Map % Complete']), 0, -1) / 100.0;
                $sMsg .= '{C.' . $this->_getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
                       . '{C.info}, reduces completed: ';

                $sRatioMsg = $aSubJob['Reduces Completed'] . '/' . $aSubJob['Reduce Total']
                      . ' (' . $aSubJob['Reduce % Complete'] . ')';
                $fRatio = substr(trim($aSubJob['Reduce % Complete']), 0, -1) / 100.0;
                $sMsg .= '{C.' . $this->_getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
                $this->_oLogger->log(LogLevel::INFO, $sMsg);
            }
        }

        $this->_oLogger->log(LogLevel::INFO, '---');
    }

    public function displayJobSummary (array $aJob, $sRawSummary, array $aErrorsMsg, array $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsection}Summary+++');

        if ($iMaxTs > 0) {
            $iMaxTSWithMargin = round($iMaxTs*1.01);
            $iMaxNbTasksWithMargin = 5*(floor($iMaxNbTasks*1.12/5) + 1);
            $sOutput = '/tmp/php-emr_' . md5(time().rand()) . '_tasktimeline.png';
            $sCmd = "gnuplot -e \"csv='$sGnuplotData'\" -e \"output='$sOutput'\""
                  . " -e \"maxts='$iMaxTSWithMargin'\" -e \"maxnbtasks='$iMaxNbTasksWithMargin'\""
                  . ' ' . $GLOBALS['aConfig']['inc_dir'] . "/plot.script";
            Helpers::exec($sCmd);
            $this->_oLogger->log(LogLevel::INFO, 'Task timeline: ' . $sOutput);
        } else {
            $this->_oLogger->log(LogLevel::INFO, 'No data for task timeline.');
        }

        $sStatus = $aJob['ExecutionStatusDetail']['State'];
        if ( ! in_array($sStatus, array('COMPLETED', 'TERMINATED', 'FAILED', 'SHUTTING_DOWN'))) {
            $this->_oLogger->log(LogLevel::INFO, 'Waiting end of job flow for both stats and DAG…');

        } else {
            if (count($aS3LogSteps) == 0) {
                $this->_oLogger->log(LogLevel::INFO, 'No log found!');
            } else {
                $this->_displayJobStats($sRawSummary);
                $this->_displayJobDAG($sRawSummary);
                $this->_displayErrors($aErrorsMsg);
            }
        }

        $this->_oLogger->log(LogLevel::INFO, '---');
    }

    private function _displayErrors (array $aErrorsMsg)
    {
        if (count($aErrorsMsg) > 0) {
            $this->_oLogger->log(LogLevel::INFO, '{C.subsubsection}Errors+++');
            foreach ($aErrorsMsg as $sErrorMsg) {
                $this->_oLogger->log(LogLevel::ERROR, $sErrorMsg);
            }
            $this->_oLogger->log(LogLevel::INFO, '---');
        }
    }

    private function _displayJobStats ($sRawSummary)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsubsection}Job Stats (time in seconds)+++');
        if (preg_match('/^Job Stats \(time in seconds\):\n(.*?)\n\n/sm', $sRawSummary, $aMatches) === 1) {
            $aLines = explode("\n", $aMatches[1]);
            // JobId, Maps, Reduces, MaxMapTime, MinMapTIme, AvgMapTime, MaxReduceTime, MinReduceTime,
            // AvgReduceTime, Alias, Feature, Outputs
            $aRawHeaders = explode("\t", $aLines[0]);
            unset($aLines[0]);
            $aHeaders = array('JobId', '#,Min/Avg/Max Maps', '#,Min/Avg/Max Reduces', 'Alias', 'Feature', 'Outputs');
            $aData = array($aHeaders);
            foreach ($aLines as $aLine) {
                $aRawValues = explode("\t", $aLine);
                $aVal = array_combine($aRawHeaders, $aRawValues);
                $aRowToDisplay = array(
                    $aVal['JobId'],
                    $aVal['Maps'] . ',' . $aVal['MinMapTIme'] . '/' . $aVal['AvgMapTime'] . '/' . $aVal['MaxMapTime'],
                    $aVal['Reduces'] . ',' . $aVal['MinReduceTime'] . '/' . $aVal['AvgReduceTime'] . '/' . $aVal['MaxReduceTime'],
                    $aVal['Alias'],
                    $aVal['Feature'],
                    $aVal['Outputs']
                );
                $aData[] = $aRowToDisplay;
            }
            $this->_oLogger->log(LogLevel::INFO, implode("\n", $this->_renderArray($aData, '  ')) . '---');
        } else {
            $this->_oLogger->log(LogLevel::INFO, 'No stats found!---');
        }
    }

    private function _displayJobDAG ($sRawSummary)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsubsection}Job DAG+++');
        if (preg_match('/^Job DAG:\n(job_\d{12}_)(.*?)\n\n/sm', $sRawSummary, $aMatches) === 1) {
            $sJobPrefix = $aMatches[1];
            $sDAG = $aMatches[2];
            $sDAG = str_replace($sJobPrefix, '', $sDAG);
            $sDAG = str_replace(",\n", ', ', $sDAG);
            $sDAG = preg_replace('/\s*->\s*/', ' ➜ ', $sDAG);
            $this->_oLogger->log(LogLevel::INFO, "($sJobPrefix) $sDAG---");
        } else {
            $this->_oLogger->log(LogLevel::INFO, 'No DAG found!---');
        }
    }

    // first row = headers
    //
    private function _renderArray (array $aData, $sInnerSeparator)
    {
        $aMaxColSize = array();
        foreach ($aData as $aRow) {
            foreach ($aRow as $iIdx => $sValue) {
                if ( ! isset($aMaxColSize[$iIdx]) || strlen($sValue) > $aMaxColSize[$iIdx]) {
                    $aMaxColSize[$iIdx] = strlen($sValue);
                }
            }
        }

        $aResultArray = array();
        $iNbCol = count($aMaxColSize);
        foreach ($aData as $aRow) {
            $aResultRow = array();
            foreach ($aRow as $iIdx => $sValue) {
                $aResultRow[] = str_pad($sValue, $aMaxColSize[$iIdx], ' ');
            }
            $aResultArray[] = implode($sInnerSeparator, $aResultRow);
        }
        return $aResultArray;
    }

    public function displayHadoopInputFiles (array $aInputFiles)
    {
        $this->_oLogger->log(LogLevel::INFO, '{C.subsubsection}Hadoop input files+++');
        foreach ($aInputFiles as $sInputFile) {
            $this->_oLogger->log(LogLevel::INFO, $sInputFile);
        }
        $this->_oLogger->log(LogLevel::INFO, '(' . count($aInputFiles) . ' files)---');
    }
}
