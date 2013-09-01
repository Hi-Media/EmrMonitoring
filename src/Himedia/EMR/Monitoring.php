<?php

namespace Himedia\EMR;

use Psr\Log\LoggerInterface;
use GAubry\Helpers\Helpers;

/**
 * Retrieve data from various external resources.
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
class Monitoring
{

    /**
     * Logger
     * @var \Psr\Log\LoggerInterface
     */
    private $oLogger;

    /**
     * Extract Amazon Elastic MapReduce Pricing from JSON stream
     * used by http://aws.amazon.com/elasticmapreduce/pricing/.
     * @var \Himedia\EMR\EMRInstancePrices
     */
    private $oEMRInstancePrices;

    /**
     * Default configuration.
     * @var array
     * @see $aConfig
     */
    private static $aDefaultConfig = array(
        'ec2_api_tools_dir' => '/path/to/ec2-api-tools-dir',
        'aws_access_key'    => '…',
        'aws_secret_key'    => '…',
        'emr_cli_bin'       => '/path/to/elastic-mapreduce',
        'ssh_options'       =>
            '-o ServerAliveInterval=10 -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes',
        'shell'             => '/bin/bash',
        'inc_dir'           => '/path/to/inc'
    );

    /**
     * Current configuration.
     * @var array
     */
    private $aConfig;

    /**
     * Constructor.
     *
     * @param LoggerInterface $oLogger
     * @param EMRInstancePrices $oEMRInstancePrices
     * @param array $aConfig configuration, see $aDefaultConfig
     */
    public function __construct (
        LoggerInterface $oLogger,
        EMRInstancePrices $oEMRInstancePrices,
        array $aConfig = array()
    ) {
        $this->oLogger = $oLogger;
        $this->oEMRInstancePrices = $oEMRInstancePrices;
        $this->aConfig = Helpers::arrayMergeRecursiveDistinct(self::$aDefaultConfig, $aConfig);
    }

    /**
     * Lists all job flows created in the last 2 days.
     *
     * @return array Array of string "job-id    status    master-node    name".
     */
    public function getAllJobs ()
    {
        $sCmd = $this->aConfig['emr_cli_bin']
              . ' --access-id ' . $this->aConfig['aws_access_key']
              . ' --private-key ' . $this->aConfig['aws_secret_key']
              . ' --list --all --no-step';
        $aRawResult = $this->exec($sCmd);
        return $aRawResult;
    }

    /**
     * Returns detailed description of the specified jobflow.
     *
     * @param string $sJobFlowID jobflow id, e.g. 'j-3PEQM17A7419J'
     * @param string $sSSHTunnelPort port used to establish a connection to the master node
     * and retrieve data from the Hadoop jobtracker
     * @return array detailed description of the specified jobflow, see resources/job.log for the job array structure
     * @see resources/job.log for the returned job array structure
     */
    public function getJobFlow ($sJobFlowID, $sSSHTunnelPort)
    {
        $sCmd = $this->aConfig['emr_cli_bin']
              . ' --access-id ' . $this->aConfig['aws_access_key']
              . ' --private-key ' . $this->aConfig['aws_secret_key']
              . " --describe --jobflow $sJobFlowID";
        $aRawResult = $this->exec($sCmd);
        $aDesc = json_decode(implode("\n", $aRawResult), true);
        $aJob = $aDesc['JobFlows'][0];
        $this->computeElapsedTimes($aJob['ExecutionStatusDetail']);

        $aJob['Instances']['MaxTotalPrice'] = 0;
        foreach ($aJob['Instances']['InstanceGroups'] as $iIdx => $aJobIGroup) {
            $sRegion = substr($aJob['Instances']['Placement']['AvailabilityZone'], 0, -1);
            list($sInstanceType, $sSize) = explode('.', $aJobIGroup['InstanceType']);
            $fPrice = $this->oEMRInstancePrices->getUSDPrice($sRegion, $sInstanceType, $sSize);
            $aJob['Instances']['InstanceGroups'][$iIdx]['OnDemandPrice'] = $fPrice;

            if ($aJobIGroup['Market'] == 'SPOT') {
                $sZone = $aJob['Instances']['Placement']['AvailabilityZone'];
                try {
                    $fPrice = $this->getSpotInstanceCurrentPricing($aJobIGroup['InstanceType'], $sZone);
                    $aJob['Instances']['InstanceGroups'][$iIdx]['AskPriceError'] = null;
                } catch (\RuntimeException $oException) {
                    $fPrice = 0;
                    $aJob['Instances']['InstanceGroups'][$iIdx]['AskPriceError'] = $oException;
                }
                $aJob['Instances']['InstanceGroups'][$iIdx]['AskPrice'] = $fPrice;
                $fPrice = min($fPrice, $aJob['Instances']['InstanceGroups'][$iIdx]['OnDemandPrice']);
            }

            $this->computeElapsedTimes($aJob['Instances']['InstanceGroups'][$iIdx]);
            if (! empty($aJob['Instances']['InstanceGroups'][$iIdx]['ElapsedTimeToEndDateTime']) && ! empty($fPrice)) {
                $iRoundedHours = ceil($aJob['Instances']['InstanceGroups'][$iIdx]['ElapsedTimeToEndDateTime']/3600);
                $aJob['Instances']['MaxTotalPrice'] += $iRoundedHours*$fPrice*$aJobIGroup['InstanceRequestCount'];
            }
        }

        foreach ($aJob['Steps'] as $iKey => $aJobStep) {
            $this->computeElapsedTimes($aJob['Steps'][$iKey]['ExecutionStatusDetail']);
            if ($aJobStep['StepConfig']['Name'] == 'Run Pig Script') {

                $aArgs = $aJobStep['StepConfig']['HadoopJarStep']['Args'];
                $sArgs = implode(', ', $aArgs);

                // PigScript
                preg_match('/, -f, ([^,]+)/i', $sArgs, $aMatches);
                $aJob['Steps'][$iKey]['PigScript'] = $aMatches[1];

                // Extract Pig parameters
                preg_match_all("/, -p, ([A-Za-z_-]+)=('[^']+'|[^'][^,]+)/i", $sArgs, $aMatches, PREG_SET_ORDER);
                $aPigParams = array('INPUT' => '', 'OUTPUT' => '');
                foreach ($aMatches as $aMatch) {
                    if (substr($aMatch[2], 0, 1) == "'") {
                        $aMatch[2] = substr($aMatch[2], 1, -1);
                    }
                    $aPigParams[$aMatch[1]] = $aMatch[2];
                }

                // PigInput & PigInputSize
                if (! empty($aPigParams['INPUT'])) {
                    $sSize = $this->getS3ObjectSize($aPigParams['INPUT']);
                    $sPigInputSize = ($sSize == '0' ? '–' : $sSize);
                } else {
                    $sPigInputSize = '–';
                }
                $aJob['Steps'][$iKey]['PigInput'] = $aPigParams['INPUT'];
                $aJob['Steps'][$iKey]['PigInputSize'] = $sPigInputSize;

                // PigOutput & PigOutputSize
                if (! empty($aPigParams['OUTPUT'])) {
                    $sSize = $this->getS3ObjectSize($aPigParams['OUTPUT'] . '/part-r-*');
                    $sPigOutputSize = ($sSize == '0' ? '–' : $sSize);
                } else {
                    $sPigOutputSize = '–';
                }
                $aJob['Steps'][$iKey]['PigOutput'] = $aPigParams['OUTPUT'];
                $aJob['Steps'][$iKey]['PigOutputSize'] = $sPigOutputSize;

                // Save others parameters
                unset($aPigParams['INPUT']);
                unset($aPigParams['OUTPUT']);
                $aJob['Steps'][$iKey]['PigOtherParameters'] = $aPigParams;

                // ClusterSummary & SubJobsSummary
                if ($aJobStep['ExecutionStatusDetail']['State'] == 'RUNNING') {
                    $this->openSSHTunnel($aJob, $sSSHTunnelPort);
                    list($aClusterSummary, $aSubJobsSummary, $sError) =
                        $this->getHadoopJobTrackerContent($sSSHTunnelPort);
                    $aJob['Steps'][$iKey]['ClusterSummary'] = $aClusterSummary;
                    $aJob['Steps'][$iKey]['SubJobsSummary'] = $aSubJobsSummary;
                    $aJob['Steps'][$iKey]['Error'] = $sError;
                }
            }
        }
        return $aJob;
    }

    /**
     * Returns size in bytes of S3 objects concerned by the specified $sPigPattern.
     *
     * @TODO http://stackoverflow.com/questions/3515481/ \
     *       pig-latin-load-multiple-files-from-a-date-range-part-of-the-directory-structur
     *
     * @param string $sPigPattern Name of a S3 file or directory, may use Hadoop-supported globing
     * @see http://pig.apache.org/docs/r0.7.0/piglatin_ref2.html#LOAD
     * @see http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html \
     *      #globStatus(org.apache.hadoop.fs.Path)
     */
    private function getS3ObjectSize ($sPigPattern)
    {
        if (strrpos($sPigPattern, '*') > strrpos($sPigPattern, '/')) {
            $sFolder = substr($sPigPattern, 0, strrpos($sPigPattern, '/')+1);
            $sPattern = str_replace(array('.', '*'), array('\.', '.*'), $sPigPattern);
            $sCmd = sprintf(
                "s3cmd ls %s | grep '%s' | awk 'BEGIN {sum=0} {sum+=$3} END {printf(\"%%.3f\", sum/1024/1024)}'",
                $sFolder,
                $sPattern
            );
        } else {
            $sCmd = $this->aConfig['inc_dir'] . "/getS3ObjectSize.sh '$sPigPattern'";
        }
        $aResult = $this->exec($sCmd);
        if ($aResult[0] == '0.000') {
            $sSize = '0';
        } else {
            $iSize = round($aResult[0]);
            if ($iSize == 0) {
                $sSize = '< 1 MiB';
            } elseif ($iSize >= 1000) {
                $sSize = round($iSize / 1024, 1) . ' GiB';
            } else {
                $sSize = $iSize . ' MiB';
            }
        }
        return $sSize;
    }

    /**
     * Opens if needed a new SSH tunnel to the master node.
     * @TODO test with: netcat -z localhost 80 => 0/1
     *
     * @param array $aJob detailed description of a jobflow.
     * @param string $sSSHTunnelPort port used to establish a connection to the master node
     * and retrieve data from the Hadoop jobtracker
     * @see resources/job.log for the job array structure
     */
    private function openSSHTunnel (array $aJob, $sSSHTunnelPort)
    {
        $sCmd = 'ps fax | grep -v \'ps fax\' | grep ssh'
              . ' | grep \'' . $aJob['Instances']['MasterPublicDnsName'] . '\' | wc -l';
        $aRawResult = $this->exec($sCmd);
        if ($aRawResult[0] == 0) {
            $sCmd = 'ssh -N -L ' . $sSSHTunnelPort . ':localhost:9100'
                . ' hadoop@' . $aJob['Instances']['MasterPublicDnsName']
                . ' ' . $this->aConfig['ssh_options'] . ' > /dev/null 2>&1 &';
            $this->exec($sCmd);
            sleep(3);
        }
    }

    /**
     * Retrieves content of the Hadoop jobtracker running on the master node.
     *
     * @param $sSSHTunnelPort port used to establish a connection to the master node
     * and retrieve data from the Hadoop jobtracker
     * @return array ($aClusterSummary, $aSubJobsSummary, $sError)
     */
    private function getHadoopJobTrackerContent ($sSSHTunnelPort)
    {
        $aClusterSummary = array();
        $aSubJobsSummary = array();
        $sError = '';

        $oDomDoc = new \DOMDocument();
        try {
            $sHtmlContent = file_get_contents("http://localhost:$sSSHTunnelPort/jobtracker.jsp");
        } catch (\ErrorException $oException) {
            $sError = $oException->getMessage();
        }

        if (empty($sError)) {
            libxml_use_internal_errors(true);
            $oDomDoc->loadHTML($sHtmlContent);
            libxml_clear_errors();
            $xpath = new \DOMXPath($oDomDoc);

            $oAllTrs = $xpath->query('//table/tr[th="Running Map Tasks"]/following-sibling::tr[1]');
            if (! empty($oAllTrs)) {
                $oTr = $oAllTrs->item(0);
                foreach ($oTr->childNodes as $cell) {
                    $aClusterSummary[] = $cell->nodeValue;
                }
            }

            $aSubJobKeys = array(
                'Jobid', 'Started', 'Priority', 'User', 'Name', 'Map % Complete', 'Map Total',
                'Maps Completed', 'Reduce % Complete', 'Reduce Total', 'Reduces Completed',
                'Job Scheduling Information', 'Diagnostic Info'
            );
            $oAllTrs = $xpath->query('//table[preceding-sibling::h2[@id="running_jobs"]]/tr');
            if ($oAllTrs->length > 0) {
                foreach ($oAllTrs as $oTr) {
                    $aSubJobValues = array();
                    foreach ($oTr->childNodes as $cell) {
                        $aSubJobValues[] = $cell->nodeValue;
                    }
                    if (count($aSubJobValues) == count($aSubJobKeys) && $aSubJobValues[0] != 'Jobid') {
                        $aSubJob = array_combine($aSubJobKeys, array_map('trim', $aSubJobValues));
                        $aSubJobsSummary[$aSubJob['Jobid']] = $aSubJob;
                    }
                }
                ksort($aSubJobsSummary);
            }
        }

        return array($aClusterSummary, $aSubJobsSummary, $sError);
    }

    /**
     * Returns current EC2 pricing of the specified spot instances type.
     *
     * @param string $sInstanceType
     * @param string $sZone
     * @return float current EC2 pricing of the specified spot instances group.
     */
    private function getSpotInstanceCurrentPricing ($sInstanceType, $sZone)
    {
        $fCurrentPricing = '';
        if (! empty($this->aConfig['ec2_api_tools_dir']) && ! empty($sZone)) {
            $sCmd = $this->aConfig['shell'] . ' -c "set -o pipefail && '
                  . $this->aConfig['ec2_api_tools_dir']
                  . '/bin/ec2-describe-spot-price-history'
                  . ' --aws-access-key ' . $this->aConfig['aws_access_key']
                  . ' --aws-secret-key ' . $this->aConfig['aws_secret_key']
                  . ' --region ' . substr($sZone, 0, -1)
                  . ' --instance-type ' . $sInstanceType
                  . ' --start-time ' . date("Y-m-d") . 'T00:00:00.000Z '
                  . ' --product-description Linux/UNIX'
                  . ' --availability-zone ' . $sZone
                  . ' | head -n1 | cut -f2"';
            $aResult = $this->exec($sCmd);
            $fCurrentPricing = (float)$aResult[0];
        }
        return $fCurrentPricing;
    }

    /**
     * Computes elapsed times between creation, start and end datetime.
     * Compares to now if a value is missing.
     *
     * Input array structure: Array(
     *     [CreationDateTime] => 1370507855
     *     [StartDateTime] => 1370508203
     *     [EndDateTime] => 1370509068
     * )
     *
     * Output array structure: Array(
     *     [CreationDateTime] => 1370507855
     *     [StartDateTime] => 1370508203
     *     [EndDateTime] => 1370509068
     *     [ElapsedTimeToStartDateTime] => 348
     *     [ElapsedTimeToEndDateTime] => 865
     * )
     *
     * @param array &$aDates
     */
    private function computeElapsedTimes (array &$aDates)
    {
        if ($aDates['CreationDateTime'] === null) {
            $aDates['ElapsedTimeToStartDateTime'] = null;
            $aDates['ElapsedTimeToEndDateTime'] = null;

        } else {
            $oCreationDate = new \DateTime('@' . (int)$aDates['CreationDateTime']);
            $mTs = ($aDates['StartDateTime'] === null ? null : '@' . (int)$aDates['StartDateTime']);
            $oStartDate = new \DateTime($mTs);
            $aDates['ElapsedTimeToStartDateTime'] =
                max(0, $oStartDate->getTimestamp() - $oCreationDate->getTimestamp());

            if ($aDates['StartDateTime'] === null) {
                $aDates['ElapsedTimeToEndDateTime'] = null;
            } else {
                $mTs = ($aDates['EndDateTime'] === null ? null : '@' . (int)$aDates['EndDateTime']);
                $oEndDate = new \DateTime($mTs);
                $aDates['ElapsedTimeToEndDateTime'] =
                    max(0, $oEndDate->getTimestamp() - $oStartDate->getTimestamp());
            }
        }
    }

    /**
     * Returns ordered list containing:
     *   – string $sSummary content of job stats section of s3://path/to/steps/stderr files
     *   – array $aErrorsMsg list of error messages (string)
     *   – array $aS3LogSteps list of s3://path/to/steps/stderr pathes
     *   – int $iMaxTs elapsed time in seconds since start of job
     *   – int $iMaxNbTasks max number of effective simultaneous tasks
     *   – string $sGnuplotData path to CSV file containing data for gnuplot
     *
     * @param string $sJobFlowID jobflow id, e.g. 'j-3PEQM17A7419J'
     * @param array $aJob detailed description of a jobflow.
     * @param string $sTmpPath
     * @see resources/job.log for the job array structure
     * @return array array($sSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData)
     */
    public function getLogSummary ($sJobFlowID, array $aJob, $sTmpPath)
    {
        $sSummary = '';
        $aErrorMsg = array();
        $sLogURI = str_replace('s3n://', 's3://', $aJob['LogUri']);

        $aS3LogSteps = array();
        if (in_array($aJob['ExecutionStatusDetail']['State'], array('COMPLETED', 'TERMINATED', 'FAILED'))) {

            // Get step logs
            $sLogURISteps = $sLogURI . "$sJobFlowID/steps/";
            $sCmd = "s3cmd ls '$sLogURISteps' | grep DIR | sed 's/ *DIR *//'";
            $aS3LogSteps = $this->exec($sCmd);
            if (count($aS3LogSteps) > 0) {
                foreach ($aS3LogSteps as $sStepURI) {
                    $sTmpFilename = $sTmpPath . '/tmp-log-summary_' . md5(time().rand());
                    $sCmd = "s3cmd get '{$sStepURI}stderr' '$sTmpFilename'";
                    $this->exec($sCmd);
                    $sContent = file_get_contents($sTmpFilename);
                    $sDatePattern = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}';
                    $sErrorPattern = '/^(' . $sDatePattern . '\s\[[^]]+\]\sERROR\s.*?)'
                                   . '(?:^' . $sDatePattern . '\s\[[^]]+\]|^Command exiting with ret \'255\'$)/sm';
                    if (preg_match('/^(Job Stats \(time in seconds\):.*+)/sm', $sContent, $aMatches) === 1) {
                        $sSummary = $aMatches[1];
                    } elseif (preg_match_all($sErrorPattern, $sContent, $aMatches) != 0) {
                        foreach ($aMatches[1] as $sRow) {
                            $aErrorMsg[] = trim($sRow);
                        }
                    }
                    unlink($sTmpFilename);
                }
            }
        }

        $aLocalJobLogPaths = array();
        $iMaxTs = 0;
        $iMaxNbTasks = 0;
        $sGnuplotData = '';

        // Get raw job logs
        $sLogURIJobs = $sLogURI . "$sJobFlowID/jobs/";
        // @TODO s3cmd sync --check-md5 --no-progress --no-delete-removed --exclude='*' --rinclude='.pig$' \
        // 's3://appnexus-us/hadoop-logs/j-1FDGYZI6HJFB7/jobs/' '/tmp/testsync'
        $sCmd = "s3cmd ls '$sLogURIJobs' | grep -v '.xml' | awk '{print $4}' | sort";
        $aRawResult = $this->exec($sCmd);
        $aLocalJobLogPaths = array();
        foreach ($aRawResult as $iIdx => $sS3JobLogPath) {
            $sLocalJobLogPath = $sTmpPath . '/log_job' . ($iIdx+1);
            $sCmd = "s3cmd get '$sS3JobLogPath' '$sLocalJobLogPath'";
            $this->exec($sCmd);
            $aLocalJobLogPaths[] = $sLocalJobLogPath;
        }

        $sGnuplotData = $sTmpPath . '/gnuplot_result.csv';
        if (count($aLocalJobLogPaths) > 0) {
            list($iMaxTs, $iMaxNbTasks) = $this->extractHistory($aLocalJobLogPaths, $sGnuplotData);
        }

        return array($sSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData);
    }

    /**
     * Extracts history from XML files into s3://logURI/<sJobFlowID>/jobs/
     * and generate a CSV timeline as follows:
     * <pre>
     *     time maps shuffle merge reduce
     *     0 1 0 0 0
     *     1 1 0 0 0
     *     …
     *     291 51 8 0 0
     *     …
     *     649 0 0 0 0
     * </pre>
     *
     * @param array $aLocalJobLogPaths list of file to extract history from
     * @param string $sDestPath CSV in which save timeline
     * @return array a pair containing elapsed time in seconds since start of job (int)
     * and max number of effective simultaneous tasks (int)
     */
    private function extractHistory (array $aLocalJobLogPaths, $sDestPath)
    {
        $aMapStartTime = array(-1 => PHP_INT_MAX);
        $aMapEndTime = array(-1 => 0);
        $aReduceStartTime = array(-1 => PHP_INT_MAX);
        $aReduceShuffleTime = array(-1 => 0);
        $aReduceSortTime = array(-1 => 0);
        $aReduceEndTime = array(-1 => 0);

        foreach ($aLocalJobLogPaths as $sSrcPath) {
            $rHandle = fopen($sSrcPath, 'r');
            while (($sLine = fgets($rHandle, 10000)) !== false) {
                $aWords = explode(' ', $sLine, 2);
                $sEvent = $aWords[0];
                if ($sEvent == 'MapAttempt') {
                    $aAttrs = $this->parseAttributes($aWords[1]);
                    if (isset($aAttrs['START_TIME'])) {
                        $aMapStartTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['START_TIME']);
                    } elseif (isset($aAttrs['FINISH_TIME'])) {
                        $aMapEndTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['FINISH_TIME']);
                    }
                } elseif ($sEvent == 'ReduceAttempt') {
                    $aAttrs = $this->parseAttributes($aWords[1]);
                    if (isset($aAttrs['START_TIME'])) {
                        $aReduceStartTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['START_TIME']);
                    } elseif (isset($aAttrs['SHUFFLE_FINISHED'])
                              && isset($aAttrs['SORT_FINISHED'])
                              && isset($aAttrs['FINISH_TIME'])
                    ) {
                        $aReduceShuffleTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['SHUFFLE_FINISHED']);
                        $aReduceSortTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['SORT_FINISHED']);
                        $aReduceEndTime[$aAttrs['TASKID']] = $this->timestampToInt($aAttrs['FINISH_TIME']);
                    }
                }
            }
            fclose($rHandle);
        }

        $aRunningMaps = array();
        $aShufflingReduces = array();
        $aSortingReduces = array();
        $aRunningReduces = array();

        $iStartTime = min(min($aMapStartTime), min($aReduceStartTime));
        $iEndTime = max(max($aMapEndTime), max($aReduceEndTime));

        for ($t=$iStartTime; $t<=$iEndTime; $t++) {
            $aRunningMaps[$t] = 0;
            $aShufflingReduces[$t] = 0;
            $aSortingReduces[$t] = 0;
            $aRunningReduces[$t] = 0;
        }

        unset($aMapStartTime[-1]);
        foreach (array_keys($aMapStartTime) as $sMap) {
            $iMaxTime = (isset($aMapEndTime[$sMap]) ? $aMapEndTime[$sMap] : $iEndTime);
            for ($t=$aMapStartTime[$sMap]; $t<$iMaxTime; $t++) {
                $aRunningMaps[$t] += 1;
            }
        }
        unset($aReduceStartTime[-1]);
        foreach (array_keys($aReduceStartTime) as $sReduce) {
            $iMaxTime = (isset($aReduceShuffleTime[$sReduce]) ? $aReduceShuffleTime[$sReduce] : $iEndTime);
            for ($t=$aReduceStartTime[$sReduce]; $t<$iMaxTime; $t++) {
                $aShufflingReduces[$t] += 1;
            }
            $iMaxTime = (isset($aReduceSortTime[$sReduce]) ? $aReduceSortTime[$sReduce] : $iEndTime);
            for ($t=$aReduceShuffleTime[$sReduce]; $t<$iMaxTime; $t++) {
                $aSortingReduces[$t] += 1;
            }
            $iMaxTime = (isset($aReduceEndTime[$sReduce]) ? $aReduceEndTime[$sReduce] : $iEndTime);
            for ($t=$aReduceSortTime[$sReduce]; $t<$iMaxTime; $t++) {
                $aRunningReduces[$t] += 1;
            }
        }

        $rHandle = fopen($sDestPath, 'w');
        fwrite($rHandle, "time maps shuffle merge reduce\n");
        $iIncr = max(1, floor(($iEndTime - $iStartTime) / 1000));
        for ($t=$iStartTime; $t<=$iEndTime; $t+=$iIncr) {
            fprintf(
                $rHandle,
                "%d %d %d %d %d\n",
                $t - $iStartTime,
                $aRunningMaps[$t],
                $aShufflingReduces[$t],
                $aSortingReduces[$t],
                $aRunningReduces[$t]
            );
        }

        $iMaxTs = $iEndTime - $iStartTime;
        if ($iEndTime == 0) {
            $iMaxNbTasks = 0;
        } else {
            $iMaxNbTasks = max(
                max($aRunningMaps),
                max($aShufflingReduces),
                max($aSortingReduces),
                max($aRunningReduces)
            );
        }
        return array($iMaxTs, $iMaxNbTasks);
    }

    /**
     * Converts timestamps of XML files into s3://logURI/<sJobFlowID>/jobs/ into Unix timestamps
     *
     * @param string timestamps with 13 characters
     * @return int Unix timestamp (10 digits)
     * @see extractHistory()
     */
    private function timestampToInt ($sTs)
    {
        //     return round((int)substr($sTs, 0, -3) + ((int)substr($sTs, -3))/1000.0);
        return (int)substr($sTs, 0, -3);
    }

    /**
     * Convert 'key1="value1" key2="value2"…' into key/value pairs.
     *
     * @param string $sAttributes
     * @return array key/value pairs
     * @see extractHistory()
     */
    private function parseAttributes ($sAttributes)
    {
        preg_match_all('/([^=]++)="([^"]*)" */m', $sAttributes, $aMatches, PREG_PATTERN_ORDER);
        $aAttributes = array_combine($aMatches[1], $aMatches[2]);
        return $aAttributes;
    }

    /**
     * Returns list of all S3 input files really loaded by Hadoop instance of the completed $sJobFlowID.
     *
     * @param string $sJobFlowID
     * @param array $aJob detailed description of the sspecified jobflow
     * @param string $sTmpPath
     * @see resources/job.log for the job array structure
     * @return array list of all S3 input files really loaded by Hadoop instance of the completed $sJobFlowID.
     */
    public function getHadoopInputFiles ($sJobFlowID, array $aJob, $sTmpPath)
    {
        $sTmpDirname = $sTmpPath . '/hadoop-input-files';
        $sLogURI = str_replace('s3n://', 's3://', $aJob['LogUri']);
        $sLogURITasks = $sLogURI . "$sJobFlowID/task-attempts";
        $sSyncCmd = "s3cmd sync"
              . " --check-md5 --no-progress --no-delete-removed --exclude='*'"
              . " --rinclude='_m(_[0-9]+)+(\.cleanup)?/syslog'"
              . " '$sLogURITasks' '$sTmpDirname'";

        $sCmd = "mkdir -p '$sTmpDirname' && $sSyncCmd --dry-run | grep download | wc -l";
        $aResult = $this->exec($sCmd);
        $iNbS3LogFilesToDl = array_pop($aResult);
        $this->oLogger->info("{C.comment}Nb of S3 log files to download: $iNbS3LogFilesToDl");

        $this->exec($sSyncCmd);

        $sBucketURI = substr($sLogURITasks, 0, strpos($sLogURITasks, '/', 5)-1);
        $sCmd = 'grep -rP --color=never --only-matching --no-filename'
              . " \"(?<=Opening '){$sBucketURI}[^']+(?=' for reading)\" '$sTmpDirname' | sort | uniq";
        $aInputFiles = $this->exec($sCmd);
        return $aInputFiles;
    }

    /**
     * Wrapper around Helpers::exec($sCmd) adding logging.
     *
     * @param string $sCmd bash command to execute
     * @return array array filled with every line of output from the command
     * @throws \RuntimeException if shell error
     */
    private function exec ($sCmd)
    {
        $this->oLogger->debug('shell# ' . trim($sCmd, " \t"));
        return Helpers::exec($sCmd);
    }
}
