<?php

namespace Himedia\EMR;

use Psr\Log\LoggerInterface;
use GAubry\Helpers\Helpers;

/**
 * Output representations of EMR jobflows.
 *
 *
 *
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
class Rendering
{
    /**
     * Colored status of a jobflow, an instance or a step.
     * @var array
     * @see getColoredStatus()
     */
    private static $aStatusColors = array(
        'ARRESTED'      => '',
        'BOOTSTRAPPING' => 'warm_up',
        'CANCELLED'     => 'warning',
        'COMPLETED'     => 'ok',
        'ENDED'         => 'discreet_ok',
        'FAILED'        => 'error',
        'PENDING'       => '',
        'PROVISIONING'  => 'warm_up',
        'RESIZING'      => 'warm_up',
        'RUNNING'       => 'running',
        'SHUTTING_DOWN' => 'warning',
        'STARTING'      => 'warm_up',
        'TERMINATED'    => 'ok',
        'WAITING'       => 'warning',
    );

    /**
     * Logger.
     * @var \Psr\Log\LoggerInterface
     */
    private $oLogger;

    /**
     * Constructor.
     *
     * @param LoggerInterface $oLogger
     */
    public function __construct (LoggerInterface $oLogger)
    {
        $this->oLogger = $oLogger;
    }

    /**
     * Display help.
     *
     * @param string $sErrorMessage optionnal error message.
     */
    public function displayHelp ($sErrorMessage = '')
    {
        if (! empty($sErrorMessage)) {
            $this->oLogger->error("\033[4m/!\\\033[24m $sErrorMessage!");
        }
        $this->oLogger->info('{C.section}Usage+++');
        $this->oLogger->info('{C.help_cmd}emr_monitoring.php {C.info}[{C.help_opt}OPTION{C.info}]…---');
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.section}Options+++');

        $this->oLogger->info('{C.help_opt}-h{C.info}, {C.help_opt}--help+++');
        $this->oLogger->info('Display this help.---');
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}-l{C.info}, {C.help_opt}--list-all-jobflows+++');
        $this->oLogger->info('List all jobflows in the last 2 weeks.---');
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}-j{C.info}, {C.help_opt}--jobflow-id {C.help_param}<jobflowid>+++');
        $this->oLogger->info('Display statistics on any {C.help_param}<jobflowid>{C.info}, finished or in progress.');
        $this->oLogger->info(
            '⇒ to monitor a jobflow in real-time: '
            . '{C.help_cmd}watch -n10 --color emr_monitoring.php {C.help_opt}-j {C.help_param}<jobflowid>---'
        );
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}--list-input-files+++');
        $this->oLogger->info(
            'With {C.help_opt}-j{C.info}, list all S3 input files really loaded by Hadoop instance '
            . 'of the completed {C.help_param}<jobflowid>{C.info}.'
        );
        $this->oLogger->info('Disable {C.help_opt}--json{C.info}.---');
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}--json+++');
        $this->oLogger->info('With {C.help_opt}-j{C.info}, convert statistics to JSON format.');
        $this->oLogger->info('Overridden by {C.help_opt}--list-input-files{C.info}.---');
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}-p{C.info}, {C.help_opt}--ssh-tunnel-port {C.help_param}<port>+++');
        $this->oLogger->info(
            'With {C.help_opt}-j{C.info}, specify the {C.help_param}<port>{C.info} used to establish a connection'
            . ' to the master node and retrieve data from the Hadoop jobtracker.---'
        );
        $this->oLogger->info(' ');

        $this->oLogger->info('{C.help_opt}-d{C.info}, {C.help_opt}--debug+++');
        $this->oLogger->info('Enable debug mode and list all shell commands.---');

        $this->oLogger->info('--- ');
    }

    /**
     * List all job flows in the last 2 weeks.
     *
     * @param array $aAllJobs Array of string "job-id    status    master-node    name".
     */
    public function displayAllJobs (array $aAllJobs)
    {
        $this->oLogger->info("{C.section}All job flows+++");
        $this->oLogger->info(implode("\n", $aAllJobs) . '---');
    }

    /**
     * Display then name of the jobflow.
     *
     * @param string $sJobName
     */
    public function displayJobName ($sJobName)
    {
        $this->oLogger->info("{C.section}Job flow name: $sJobName+++");
    }

    /**
     * Display general status of the specified job:
     * master public DNS name, log URI, zone, normalized hours, status, init/start/end date.
     *
     * Example of rendering:
     * <pre>
     * General
     *     MasterPublicDnsName:   ec2-54-234-23-37.compute-1.amazonaws.com
     *     Log URI:               s3://bucket/hadoop-logs/
     *     Zone:                  us-east-1d
     *     Norm. hours, price:    100h, ≤ $2.16
     *     Status:                COMPLETED, Steps completed
     *     Init/start/end date:   2013-06-06 10:37:35  /  2013-06-06 10:43:23 (+00:05:48)
     *                                                 /  2013-06-06 10:57:48 (+00:14:25)
     * </pre>
     *
     * @param array $aJob
     * @see resources/job.log for the job array structure
     */
    public function displayJobGeneralStatus (array $aJob)
    {
        $this->oLogger->info('{C.subsection}General+++');

        $sMasterPublicDnsName = $aJob['Instances']['MasterPublicDnsName'];
        $sMsg = str_pad('MasterPublicDnsName: ', 23, ' ')
              . ($sMasterPublicDnsName === null ? '–' : $sMasterPublicDnsName);
        $this->oLogger->info($sMsg);

        $sLogUri = $aJob['LogUri'];
        $sMsg = str_pad('Log URI: ', 23, ' ') . ($sLogUri === null ? '–' : str_replace('s3n://', 's3://', $sLogUri));
        $this->oLogger->info($sMsg);

        $sAvailabilityZone = $aJob['Instances']['Placement']['AvailabilityZone'];
        $sMsg = str_pad('Zone: ', 23, ' ') . ($sAvailabilityZone === null ? '–' : $sAvailabilityZone);
        $this->oLogger->info($sMsg);

        $sNormInstanceHours = $aJob['Instances']['NormalizedInstanceHours'];
        $fMaxTotalPrice = $aJob['Instances']['MaxTotalPrice'];
        $sMsg = str_pad('Norm. hours, price: ', 23, ' ')
              . (empty($sNormInstanceHours) ? '–' : $sNormInstanceHours .'h')
              . ', {C.price}' . ($fMaxTotalPrice == 0 ? '–' : '≤ $' . $fMaxTotalPrice);
        $this->oLogger->info($sMsg);
        $this->displayStatusAndDates($aJob['ExecutionStatusDetail']);

        $this->oLogger->info('---');
    }

    /**
     * Display status, init/start/end date and elapsed time of a job section.
     *
     * Example of rendering:
     * <pre>
     *     Status:                COMPLETED
     *     Init/start/end date:   2013-06-10 13:06:39  /  2013-06-10 13:11:09 (+00:04:30)
     *                                                 /  2013-06-10 13:11:21 (+00:00:12)
     * </pre>
     *
     * @param array $aJobSection job section with following structure:
     * Array(
     *     [StartDateTime] => 1370862669
     *     [CreationDateTime] => 1370862399
     *     [LastStateChangeReason] => Steps completed with errors
     *     [EndDateTime] => 1370862825
     *     [State] => COMPLETED
     *     [ElapsedTimeToStartDateTime] => 270
     *     [ElapsedTimeToEndDateTime] => 156
     * )
     */
    private function displayStatusAndDates (array $aJobSection)
    {
        $sStatus = $aJobSection['State'];
        $sStateChangeReason = $aJobSection['LastStateChangeReason'];
        $sColoredStatus = $this->getColoredStatus($sStatus, $sStateChangeReason);
        $this->oLogger->info($sColoredStatus);
        $sMsg = str_pad('Init/start/end date: ', 23, ' ');

        $aAllDateTypes = array('CreationDateTime', 'StartDateTime', 'EndDateTime');
        foreach ($aAllDateTypes as $sDateType) {
            if ($sDateType != 'CreationDateTime') {
                $sMsg .= '{C.section}  /  {C.info}';
            }
            $sMsg .= ($aJobSection[$sDateType] === null ? '–' : date('Y-m-d H:i:s', (int)$aJobSection[$sDateType]));
            if (! empty($aJobSection["ElapsedTimeTo$sDateType"])) {
                $sMsg .= ' {C.comment}('
                       . ($aJobSection[$sDateType] === null ? '≈ ' : '')
                       . '+' . gmdate('H:i:s', $aJobSection["ElapsedTimeTo$sDateType"]) . ')';
            }
        }

        $this->oLogger->info($sMsg);
    }

    /**
     * Display colored status of a jobflow, an instance or a step.
     *
     * @param string $sStatus
     * @param string $sStateChangeReason
     * @see $aStatusColors
     */
    private function getColoredStatus ($sStatus, $sStateChangeReason)
    {
        $sMsg = str_pad('Status: ', 23, ' ')
              . (! empty(self::$aStatusColors[$sStatus]) ? '{C.' . self::$aStatusColors[$sStatus] . '}' : '')
              . $sStatus . (! empty($sStateChangeReason) ? ", $sStateChangeReason" : '');
        return $sMsg;
    }

    /**
     * Display job instances, e.g. master/core/task instance group.
     *
     * Example of rendering:
     * <pre>
     * Instances
     *     Master Instance Group
     *         Detail:                MASTER, ON_DEMAND, 0/1 m1.xlarge, $0.6 /h/instance
     *         Status:                ENDED, Job flow terminated
     *         Init/start/end date:   2013-06-10 13:06:39  /  2013-06-10 13:10:00 (+00:03:21)
     *                                                     /  2013-06-10 13:13:43 (+00:03:43)
     *     Core Instance Group
     *         …
     *     Task Instance Group
     *         …
     * </pre>
     *
     * @param array $aJob
     * @see resources/job.log for the job array structure
     */
    public function displayJobInstances (array $aJob)
    {
        $this->oLogger->info('{C.subsection}Instances+++');

        foreach ($aJob['Instances']['InstanceGroups'] as $aJobIGroup) {
            $sName = $aJobIGroup['Name'];
            $this->oLogger->info("{C.subsubsection}$sName+++");

            $sMarket = $aJobIGroup['Market'];
            if (empty($aJobIGroup['AskPrice'])) {
                $sAskPrice = '–';
            } else {
                $sAskPrice = '$' . $aJobIGroup['AskPrice'];
                if ($aJobIGroup['AskPrice'] > $aJobIGroup['BidPrice']) {
                    $sAskPrice = "{C.raw_error}$sAskPrice{C.price}";
                } elseif ($aJobIGroup['AskPrice'] == $aJobIGroup['BidPrice']) {
                    $sAskPrice = "{C.warning}$sAskPrice{C.price}";
                }
            }
            if ($sMarket == 'SPOT') {
                $fPrice = 'bid:$' . $aJobIGroup['BidPrice'] . ' ask:' . $sAskPrice;
            } elseif (! empty($aJobIGroup['OnDemandPrice'])) {
                $fPrice = '$' . $aJobIGroup['OnDemandPrice'];
            } else {
                $fPrice = '';
            }

            $sMsg = $aJobIGroup['InstanceRunningCount'] . '/' . $aJobIGroup['InstanceRequestCount'];
            $fRatio = 1.0 * (int)$aJobIGroup['InstanceRunningCount'] / (int)$aJobIGroup['InstanceRequestCount'];
            $sDesc = str_pad('Detail: ', 23, ' ') . $aJobIGroup['InstanceRole'] . ', ' . $sMarket . ', ';
            $this->oLogger->info(
                $sDesc
                . '{C.' . $this->getColorAccordingToRatio($fRatio) . '}' . $sMsg
                . '{C.info} ' . $aJobIGroup['InstanceType']
                . ', {C.price}' . (empty($fPrice) ? '–' : $fPrice . ' /h/instance')
            );
            if (isset($aJobIGroup['AskPriceError']) && $aJobIGroup['AskPriceError'] instanceof \RuntimeException) {
                $sMsg = 'Error when fetching spot instance pricing!'
                      . "\n{C.raw_error}"
                      . str_replace("\n", "\n{C.raw_error}", $aJobIGroup['AskPriceError']);
                $this->oLogger->error($sMsg);
            }

            $this->displayStatusAndDates($aJobIGroup);
            $this->oLogger->info('---');
        }

        $this->oLogger->info('---');
    }

    /**
     * Returns color code according to specified ratio.
     * Useful for progression indicators.
     *
     * @param float $fRatio between 0 and 1, inclusive
     * @return string color code according to specified ratio.
     */
    private function getColorAccordingToRatio ($fRatio)
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

    /**
     * Display steps details of the specified job.
     *
     * Example of rendering:
     * <pre>
     * Steps
     *     Setup Hadoop Debugging
     *         Status:                COMPLETED
     *         Init/start/end date:   2013-06-10 13:06:39  /  2013-06-10 13:11:09 (+00:04:30)
     *                                                     /  2013-06-10 13:11:21 (+00:00:12)
     *     Setup Pig
     *         …
     *     Run Pig Script
     *         …
     * </pre>
     *
     * @param array $aJob
     * @see resources/job.log for the job array structure
     */
    public function displayJobSteps (array $aJob)
    {
        $this->oLogger->info('{C.subsection}Steps+++');

        $aJobSteps = $aJob['Steps'];
        foreach ($aJobSteps as $aJobStep) {
            $sName = $aJobStep['StepConfig']['Name'];
            $this->oLogger->info("{C.subsubsection}$sName+++");
            $aStepStatus = $aJobStep['ExecutionStatusDetail'];
            $this->displayStatusAndDates($aStepStatus);

            if ($sName == 'Run Pig Script') {
                $this->oLogger->info(str_pad('Script:', 23, ' ') . $aJobStep['PigScript']);

                // Input/output:
                $this->oLogger->info(
                    str_pad('Input/output (size):', 23, ' ')
                    . $aJobStep['PigInput'] . ' {C.comment}(' . $aJobStep['PigInputSize'] . ')'
                    . '{C.section}  ⇒  {C.info}'
                    . $aJobStep['PigOutput'] . ' {C.comment}(' . $aJobStep['PigOutputSize'] . ')'
                );

                // Other parameters:
                if (count($aJobStep['PigOtherParameters']) > 0) {
                    $aMsg = array();
                    foreach ($aJobStep['PigOtherParameters'] as $sName => $sValue) {
                        $aMsg[] = "$sName=$sValue";
                    }
                    $sMsg = implode(', ', $aMsg);
                } else {
                    $sMsg = '–';
                }
                $this->oLogger->info(str_pad('Other parameters:', 23, ' ') . $sMsg);

                if ($aStepStatus['State'] == 'RUNNING') {
                    $this->displaySubJobs($aJobStep);
                }
            }

            $this->oLogger->info('---');
        }

        $this->oLogger->info('---');
    }

    /**
     * Display details of a step in progress.
     *
     * @param array $aJobStep
     * @see resources/job.log for the job array structure
     */
    private function displaySubJobs (array $aJobStep)
    {
        if (count($aJobStep['ClusterSummary']) > 0) {
            $aValues = $aJobStep['ClusterSummary'];
            $sMsg = str_pad('Cluster summary:', 23, ' ') . 'running maps/capacity: ';
            $sRatioMsg = $aValues[0] . '/' . $aValues[8];
            $fRatio = 1.0 * $aValues[0] / $aValues[8];
            $sMsg .= '{C.' . $this->getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
                   . '{C.info}, running reduces/capacity: ';
            $sRatioMsg =  $aValues[1] . '/' . $aValues[9];
            $fRatio = 1.0 * $aValues[1] / $aValues[9];
            $sMsg .= '{C.' . $this->getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
            $this->oLogger->info($sMsg);
        }

        $this->oLogger->info('{C.section}Jobs+++');
        $aSubJobsSummary = $aJobStep['SubJobsSummary'];
        if (! empty($aJobStep['Error'])) {
            $this->oLogger->error($aJobStep['Error']);
        } elseif (count($aSubJobsSummary) == 0) {
            $this->oLogger->info('No job found…');
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
                $sMsg .= '{C.' . $this->getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg
                       . '{C.info}, reduces completed: ';

                $sRatioMsg = $aSubJob['Reduces Completed'] . '/' . $aSubJob['Reduce Total']
                      . ' (' . $aSubJob['Reduce % Complete'] . ')';
                $fRatio = substr(trim($aSubJob['Reduce % Complete']), 0, -1) / 100.0;
                $sMsg .= '{C.' . $this->getColorAccordingToRatio($fRatio) . '}' . $sRatioMsg;
                $this->oLogger->info($sMsg);
            }
        }

        $this->oLogger->info('---');
    }

    /**
     * Display summary of a job.
     *
     * Example of rendering:
     * <pre>
     * Summary
     *     Task timeline: /tmp/php-emr_71f1a752e74c69d14732626c2276b3a8_tasktimeline.png
     *     Job Stats (time in seconds)
     *         JobId                  #,Min/Avg/Max Maps  #,Min/Avg/Max Reduces  Alias      Feature            Outputs
     *         job_201306060840_0001  120,27/246/356      20,294/305/351         A,B,C,D,E  GROUP_BY,COMBINER  …
     *     Job DAG
     *         (job_201306060840_) 0001
     * </pre>
     *
     * @param array see $aJob resources/job.log for the job array structure
     * @param string $sRawSummary content of job stats section of s3://path/to/steps/stderr files
     * @param array $aErrorsMsg list of error messages (string)
     * @param array $aS3LogSteps list of s3://path/to/steps/stderr pathes
     * @param int $iMaxTs elapsed time in seconds since start of job
     * @param int $iMaxNbTasks max number of effective simultaneous tasks
     * @param string $sGnuplotData path to CSV file containing data for gnuplot
     * @param string $sGnuplotScript gnuplot script to execute
     */
    public function displayJobSummary (
        array $aJob,
        $sRawSummary,
        array $aErrorsMsg,
        array $aS3LogSteps,
        $iMaxTs,
        $iMaxNbTasks,
        $sGnuplotData,
        $sGnuplotScript
    ) {
        $this->oLogger->info('{C.subsection}Summary+++');

        if ($iMaxTs > 0) {

            // Retrieve details of instances
            $aInstances = array();
            foreach ($aJob['Instances']['InstanceGroups'] as $aJobIGroup) {
                if ($aJobIGroup['InstanceRole'] != 'MASTER') {
                    $aInstances[] = $aJobIGroup['InstanceRequestCount'] . '×' . $aJobIGroup['InstanceType']
                                  . ($aJobIGroup['Market'] == 'SPOT' ? ' ' . strtolower($aJobIGroup['Market']) : '')
                                  . ' ' . $aJobIGroup['InstanceRole'];
                }
            }
            $sInstances = implode(', ', $aInstances);

            // Retrieve size of input/output and value of user parameters
            $sSize = '';
            $sOtherParam = '–';
            foreach ($aJob['Steps'] as $aJobStep) {
                if ($aJobStep['StepConfig']['Name'] == 'Run Pig Script') {
                    $sSize = $aJobStep['PigInputSize'] . ' ⇒ ' . $aJobStep['PigOutputSize'];

                    // Other parameters:
                    if (count($aJobStep['PigOtherParameters']) > 0) {
                        $aOtherParam = array();
                        foreach ($aJobStep['PigOtherParameters'] as $sName => $sValue) {
                            $aOtherParam[] = "$sName=$sValue";
                        }
                        $sOtherParam = str_replace('_', '\_', implode(', ', $aOtherParam));
                    }
                }
            }

            $iMaxTSWithMargin = round($iMaxTs*1.01);
            $iMaxNbTasksWMargin = 5*(floor($iMaxNbTasks*1.12/5) + 1);
            $sOutput = '/tmp/php-emr_' . md5(time().rand()) . '_tasktimeline.png';
            $sJobflowName = str_replace('_', '\_', $aJob['Name']);
            $iJobflowId = $aJob['JobFlowId'];
            $sCmd = "gnuplot -e \"csv='$sGnuplotData'; output='$sOutput'\""
                  . " -e \"maxts='$iMaxTSWithMargin'; maxnbtasks='$iMaxNbTasksWMargin'\""
                  . " -e \"; name='$sJobflowName'; jobflowid='$iJobflowId'; instances='$sInstances'\""
                  . " -e \"; size='$sSize'; otherparameters='$sOtherParam'\""
                  . ' ' . $sGnuplotScript;
            Helpers::exec($sCmd);
            $this->oLogger->info('Task timeline: ' . $sOutput);
        } else {
            $this->oLogger->info('No data for task timeline.');
        }

        $sStatus = $aJob['ExecutionStatusDetail']['State'];
        if (! in_array($sStatus, array('COMPLETED', 'TERMINATED', 'FAILED', 'SHUTTING_DOWN'))) {
            $this->oLogger->info('Waiting end of job flow for both stats and DAG…');

        } else {
            if (count($aS3LogSteps) == 0) {
                $this->oLogger->info('No log found!');
            } else {
                $this->displayJobStats($sRawSummary);
                $this->displayJobDAG($sRawSummary);
                $this->displayErrors($aErrorsMsg);
            }
        }

        $this->oLogger->info('---');
    }

    /**
     * Display error message.
     *
     * @param array $aErrorsMsg list of error messages (string)
     */
    private function displayErrors (array $aErrorsMsg)
    {
        if (count($aErrorsMsg) > 0) {
            $this->oLogger->info('{C.subsubsection}Errors+++');
            foreach ($aErrorsMsg as $sErrorMsg) {
                $this->oLogger->error($sErrorMsg);
            }
            $this->oLogger->info('---');
        }
    }

    /**
     * Display job stats from Hadoop jobtracker.
     *
     * Example of rendering:
     * <pre>
     *     Job Stats (time in seconds)
     *         JobId                  #,Min/Avg/Max Maps  #,Min/Avg/Max Reduces  Alias      Feature            Outputs
     *         job_201306060840_0001  120,27/246/356      20,294/305/351         A,B,C,D,E  GROUP_BY,COMBINER  …
     * </pre>
     *
     * @param string $sRawSummary content of job stats section of s3://path/to/steps/stderr files
     */
    private function displayJobStats ($sRawSummary)
    {
        $this->oLogger->info('{C.subsubsection}Job Stats (time in seconds)+++');
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
                    $aVal['Reduces'] . ',' . $aVal['MinReduceTime'] . '/' . $aVal['AvgReduceTime']
                        . '/' . $aVal['MaxReduceTime'],
                    $aVal['Alias'],
                    $aVal['Feature'],
                    $aVal['Outputs']
                );
                $aData[] = $aRowToDisplay;
            }
            $this->oLogger->info(implode("\n", $this->renderArray($aData, '  ')) . '---');
        } else {
            $this->oLogger->info('No stats found!---');
        }
    }

    /**
     * Display job DAG (directed acyclic graph) from Hadoop jobtracker.
     *
     * Example of rendering:
     * <pre>
     *     Job DAG
     *         (job_201306060840_) 0001
     * </pre>
     *
     * @param string $sRawSummary content of job stats section of s3://path/to/steps/stderr files
     */
    private function displayJobDAG ($sRawSummary)
    {
        $this->oLogger->info('{C.subsubsection}Job DAG+++');
        if (preg_match('/^Job DAG:\n(job_\d{12}_)(.*?)\n\n/sm', $sRawSummary, $aMatches) === 1) {
            $sJobPrefix = $aMatches[1];
            $sDAG = $aMatches[2];
            $sDAG = str_replace($sJobPrefix, '', $sDAG);
            $sDAG = str_replace(",\n", ', ', $sDAG);
            $sDAG = preg_replace('/\s*->\s*/', ' ➜ ', $sDAG);
            $this->oLogger->info("($sJobPrefix) $sDAG---");
        } else {
            $this->oLogger->info('No DAG found!---');
        }
    }

    /**
     * Return an HTML-like table in bash using PHP array.
     * First row contains headers.
     *
     * @param array $aData
     * @param string $sInnerSeparator separator between columns
     * @return array list of rows of an HTML-like table in bash
     */
    private function renderArray (array $aData, $sInnerSeparator)
    {
        $aMaxColSize = array();
        foreach ($aData as $aRow) {
            foreach ($aRow as $iIdx => $sValue) {
                if (! isset($aMaxColSize[$iIdx]) || strlen($sValue) > $aMaxColSize[$iIdx]) {
                    $aMaxColSize[$iIdx] = strlen($sValue);
                }
            }
        }

        $aResultArray = array();
        foreach ($aData as $aRow) {
            $aResultRow = array();
            foreach ($aRow as $iIdx => $sValue) {
                $aResultRow[] = str_pad($sValue, $aMaxColSize[$iIdx], ' ');
            }
            $aResultArray[] = implode($sInnerSeparator, $aResultRow);
        }
        return $aResultArray;
    }

    /**
     * Display all S3 input files really loaded by Hadoop instance of a completed <jobflowid>.
     *
     * @param array $aInputFiles
     */
    public function displayHadoopInputFiles (array $aInputFiles)
    {
        $this->oLogger->info('{C.subsubsection}Hadoop input files+++');
        foreach ($aInputFiles as $sInputFile) {
            $this->oLogger->info($sInputFile);
        }
        $this->oLogger->info('(' . count($aInputFiles) . ' files)---');
    }
}
