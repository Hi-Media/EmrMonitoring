<?php

namespace Himedia\EMR;

use GAubry\Shell\ShellInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use GAubry\Debug\Debug;
use GAubry\Tools\Tools;

class Monitoring
{

    private $_oLogger;
    private $_oShell;
    private $_oEMRInstancePrices;

    private static $_aDefaultConfig = array(
        'ec2_api_tools_dir' => '/path/to/ec2-api-tools-dir',
        'aws_access_key' => '…',
        'aws_secret_key' => '…',
        'emr_cli_bin' => '/path/to/elastic-mapreduce',
        'ssh_options' => '-o ServerAliveInterval=10 -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes',
    );

    /**
     * @var array
     */
    private $_aConfig;

    public function __construct (
        LoggerInterface $oLogger,
        ShellInterface $oShell,
        EMRInstancePrices $oEMRInstancePrices,
        array $aConfig=array())
    {
        $this->_oLogger = $oLogger;
        $this->_oShell = $oShell;
        $this->_oEMRInstancePrices = $oEMRInstancePrices;
        $this->_aConfig = Tools::arrayMergeRecursiveDistinct(self::$_aDefaultConfig, $aConfig);
    }

    /**
     * List all job flows created in the last 2 days.
     *
     * @return array Array of string "job-id    status    master-node    name".
     */
    public function getAllJobs ()
    {
        $sCmd = $this->_aConfig['emr_cli_bin']
              . ' --access-id ' . $this->_aConfig['aws_access_key']
              . ' --private-key ' . $this->_aConfig['aws_secret_key']
              . ' --list --all --no-step';
        $aRawResult = $this->_oShell->exec($sCmd);
        return $aRawResult;
    }

    public function getJobFlow ($sJobFlowID, $sSSHTunnelPort)
    {
        $sCmd = $this->_aConfig['emr_cli_bin']
              . ' --access-id ' . $this->_aConfig['aws_access_key']
              . ' --private-key ' . $this->_aConfig['aws_secret_key']
              . " --describe $sJobFlowID";
        $aRawResult = $this->_oShell->exec($sCmd);
        $aDesc = json_decode(implode("\n", $aRawResult), true);
        $aJob = $aDesc['JobFlows'][0];
        $this->_computeElapsedTimes($aJob['ExecutionStatusDetail']);

        $aJob['Instances']['MaxTotalPrice'] = 0;
        foreach ($aJob['Instances']['InstanceGroups'] as $iIdx => $aJobIGroup) {
            $sRegion = substr($aJob['Instances']['Placement']['AvailabilityZone'], 0, -1);
            list($sInstanceType, $sSize) = explode('.', $aJobIGroup['InstanceType']);
            $fPrice = $this->_oEMRInstancePrices->getUSDPrice($sRegion, $sInstanceType, $sSize);
            $aJob['Instances']['InstanceGroups'][$iIdx]['OnDemandPrice'] = $fPrice;

            if ($aJobIGroup['Market'] == 'SPOT') {
                $sZone = $aJob['Instances']['Placement']['AvailabilityZone'];
                $fPrice = $this->_getSpotInstanceCurrentPricing($aJobIGroup, $sZone);
                $aJob['Instances']['InstanceGroups'][$iIdx]['AskPrice'] = $fPrice;
            }

            $this->_computeElapsedTimes($aJob['Instances']['InstanceGroups'][$iIdx]);
            if (
                ! empty($aJob['Instances']['InstanceGroups'][$iIdx]['ElapsedTimeToEndDateTime'])
                && ! empty($fPrice)
            ) {
                $iRoundedHours = ceil($aJob['Instances']['InstanceGroups'][$iIdx]['ElapsedTimeToEndDateTime']/3600);
                $aJob['Instances']['MaxTotalPrice'] += $iRoundedHours*$fPrice*$aJobIGroup['InstanceRequestCount'];
            }
        }

        foreach ($aJob['Steps'] as $iKey => $aJobStep) {
            $this->_computeElapsedTimes($aJob['Steps'][$iKey]['ExecutionStatusDetail']);
            if ($aJobStep['StepConfig']['Name'] == 'Run Pig Script') {

                $aArgs = $aJobStep['StepConfig']['HadoopJarStep']['Args'];
                $sArgs = implode(', ', $aArgs);

                // PigScript
                preg_match('/, -f, ([^,]+)/i', $sArgs, $aMatches);
                $aJob['Steps'][$iKey]['PigScript'] = $aMatches[1];

                // PigInput & PigInputSize
                preg_match("/, -p, INPUT=('[^']+'|[^'][^,]+)/i", $sArgs, $aMatches);
                $sPigInput = $aMatches[1];
                if (substr($sPigInput, 0, 1) == "'") {
                    $sPigInput = substr($sPigInput, 1, -1);
                }
                if ( ! empty($sPigInput)) {
                    $sSize = $this->_getS3ObjectSize($sPigInput);
                    $sPigInputSize = ' {C.comment}(' . ($sSize == '0' ? '–' : $sSize) . ')';
                } else {
                    $sPigInputSize = '';
                }
                $aJob['Steps'][$iKey]['PigInput'] = $sPigInput;
                $aJob['Steps'][$iKey]['PigInputSize'] = $sPigInputSize;

                // PigOutput & PigOutputSize
                preg_match("/, -p, OUTPUT=('[^']+'|[^'][^,]+)/i", $sArgs, $aMatches);
                $sPigOutput = $aMatches[1];
                if (substr($sPigOutput, 0, 1) == "'") {
                    $sPigOutput = substr($sPigOutput, 1, -1);
                }
                if ( ! empty($sPigOutput)) {
                    $sSize = $this->_getS3ObjectSize($sPigOutput . '/part-r-*');
                    $sPigOutputSize = ' {C.comment}(' . ($sSize == '0' ? '–' : $sSize) . ')';
                } else {
                    $sPigOutputSize = '';
                }
                $aJob['Steps'][$iKey]['PigOutput'] = $sPigOutput;
                $aJob['Steps'][$iKey]['PigOutputSize'] = $sPigOutputSize;

                // ClusterSummary & SubJobsSummary
                if ($aJobStep['ExecutionStatusDetail']['State'] == 'RUNNING') {
                    $this->_openSSHTunnel($aJob, $sSSHTunnelPort);
                    list($aClusterSummary, $aSubJobsSummary) = $this->_getHadoopJobTrackerContent();
                    $aJob['Steps'][$iKey]['ClusterSummary'] = $aClusterSummary;
                    $aJob['Steps'][$iKey]['SubJobsSummary'] = $aSubJobsSummary;
                }
            }
        }
        return $aJob;
    }

    /*
     * s3://appnexus-us/input/standard_feed/2013/04/03/standard_feed_2013_04_03_*.gz
     * s3://appnexus-us/output/4
     * s3cmd du -H …
     * http://pig.apache.org/docs/r0.7.0/piglatin_ref2.html#LOAD
     * http://stackoverflow.com/questions/3515481/pig-latin-load-multiple-files-from-a-date-range-part-of-the-directory-structur
     */
    private function _getS3ObjectSize ($sPigPattern)
    {
        $sFolder = substr($sPigPattern, 0, strrpos($sPigPattern, '/')+1);
        $sPattern = str_replace(array('.', '*'), array('\.', '.*'), $sPigPattern);
        $sCmd = sprintf(
            "s3cmd ls %s | grep '%s' | awk 'BEGIN {sum=0} {sum+=$3} END {printf(\"%%.3f\", sum/1024/1024)}'",
            $sFolder,
            $sPattern
        );
        $aResult = $this->_oShell->exec($sCmd);
        if ($aResult[0] == '0.000') {
            $sSize = '0';
        } else {
            $iSize = round($aResult[0]);
            if ($iSize == 0 ) {
                $sSize = '< 1 MiB';
            } else if ($iSize >= 1000) {
                $sSize = round($iSize / 1024, 1) . ' GiB';
            } else {
                $sSize = $iSize . ' MiB';
            }
        }
        return $sSize;
    }

    private function _openSSHTunnel (array $aJob, $sSSHTunnelPort)
    {
        $sCmd = 'ps fax | grep -v \'ps fax\' | grep ssh'
              . ' | grep \'' . $aJob['Instances']['MasterPublicDnsName'] . '\' | wc -l';
        $aRawResult = $this->_oShell->exec($sCmd);
        if ($aRawResult[0] == 0) {
            $sCmd = 'ssh -N -L ' . $sSSHTunnelPort . ':localhost:9100'
                . ' hadoop@' . $aJob['Instances']['MasterPublicDnsName']
                . ' ' . $this->_aConfig['ssh_options'] . ' > /dev/null 2>&1 &';
            $this->_oShell->exec($sCmd);
            sleep(3);
            // TODO test with: netcat -z localhost 80 => 0/1
        }
    }

    private function _getHadoopJobTrackerContent ()
    {
        $dom_doc = new \DOMDocument();
        $html_file = file_get_contents('http://localhost:12345/jobtracker.jsp');
        libxml_use_internal_errors(true);
        $dom_doc->loadHTML($html_file);
        libxml_clear_errors();
        $xpath = new \DOMXPath($dom_doc);

        $aClusterSummary = array();
        $trs = $xpath->query('//table/tr[th="Running Map Tasks"]/following-sibling::tr[1]');
        if ( ! empty($trs)) {
            $tr = $trs->item(0);
            foreach($tr->childNodes as $cell) {
                $aClusterSummary[] = $cell->nodeValue;
            }
        }

        $aSubJobsSummary = array();
        $aSubJobKeys = array(
            'Jobid', 'Started', 'Priority', 'User', 'Name', 'Map % Complete', 'Map Total',
            'Maps Completed', 'Reduce % Complete', 'Reduce Total', 'Reduces Completed',
            'Job Scheduling Information', 'Diagnostic Info'
        );
        $trs = $xpath->query('//table[preceding-sibling::h2[@id="running_jobs"]]/tr');
        if ($trs->length > 0) {
            foreach ($trs as $tr) {
                $aSubJobValues = array();
                foreach($tr->childNodes as $cell) {
                    $aSubJobValues[] = $cell->nodeValue;
                }
                if (count($aSubJobValues) == count($aSubJobKeys) && $aSubJobValues[0] != 'Jobid') {
                    $aSubJob = array_combine($aSubJobKeys, array_map('trim', $aSubJobValues));
                    $aSubJobsSummary[$aSubJob['Jobid']] = $aSubJob;
                }
            }
            ksort($aSubJobsSummary);
        }

        return array($aClusterSummary, $aSubJobsSummary);
    }

    private function _getSpotInstanceCurrentPricing (array $aJobIGroup, $sZone)
    {
        $fCurrentPricing = '';
        if ( ! empty($this->_aConfig['ec2_api_tools_dir']) && ! empty($sZone)) {
            $sCmd = $this->_aConfig['ec2_api_tools_dir']
                  . '/bin/ec2-describe-spot-price-history'
                  . ' --aws-access-key ' . $this->_aConfig['aws_access_key']
                  . ' --aws-secret-key ' . $this->_aConfig['aws_secret_key']
                  . ' --region ' . substr($sZone, 0, -1)
                  . ' --instance-type ' . $aJobIGroup['InstanceType']
                  . ' --start-time ' . date("Y-m-d") . 'T00:00:00.000Z '
                  . ' --product-description Linux/UNIX'
                  . ' --availability-zone ' . $sZone
                  . ' | head -n1 | cut -f2';
            $aResult = $this->_oShell->exec($sCmd);
            $fCurrentPricing = (float)$aResult[0];
        }
        return $fCurrentPricing;
    }

    private function _computeElapsedTimes (array &$aDates)
    {
        if ($aDates['CreationDateTime'] === null) {
            $aDates['ElapsedTimeToStartDateTime'] = null;
            $aDates['ElapsedTimeToEndDateTime'] = null;

        } else {
            $oCreationDate = new \DateTime('@' . (int)$aDates['CreationDateTime']);
            $mTs = ($aDates['StartDateTime'] === null ? null : '@' . (int)$aDates['StartDateTime']);
            $oStartDate = new \DateTime($mTs);
            $aDates['ElapsedTimeToStartDateTime'] = $oStartDate->getTimestamp() - $oCreationDate->getTimestamp();

            if ($aDates['StartDateTime'] === null) {
                $aDates['ElapsedTimeToEndDateTime'] = null;
            } else {
                $mTs = ($aDates['EndDateTime'] === null ? null : '@' . (int)$aDates['EndDateTime']);
                $oEndDate = new \DateTime($mTs);
                $aDates['ElapsedTimeToEndDateTime'] = $oEndDate->getTimestamp() - $oStartDate->getTimestamp();
            }
        }
    }

    public function getLogSummary ($sJobFlowID, array $aJob)
    {
        $sSummary = '';
        $aErrorMsg = array();
        $sLogURI = str_replace('s3n://', 's3://', $aJob['LogUri']);

        $aS3LogSteps = array();
        if (in_array($aJob['ExecutionStatusDetail']['State'], array('COMPLETED', 'TERMINATED', 'FAILED'))) {

            // Get step logs
            $sLogURISteps = $sLogURI . "$sJobFlowID/steps/";
            $sCmd = "s3cmd ls '$sLogURISteps' | grep DIR | sed 's/ *DIR *//'";
            $aS3LogSteps = $this->_oShell->exec($sCmd);
            if (count($aS3LogSteps) > 0) {
                foreach ($aS3LogSteps as $sStepURI) {
                    $sTmpFilename = '/tmp' . '/php-emr_' . md5(time().rand());
                    $sCmd = "s3cmd get '{$sStepURI}stderr' '$sTmpFilename'";
                    $this->_oShell->exec($sCmd);
                    $sContent = file_get_contents($sTmpFilename);
                    if (preg_match('/^(Job Stats \(time in seconds\):.*+)/sm', $sContent, $aMatches) === 1) {
                        $sSummary = $aMatches[1];
                    } else if (preg_match_all('/^([^[]+\s\[[^]]+\]\sERROR\s.*?)(?:^[^[]+\s\[[^]]+\]|^Command exiting with ret \'255\'$)/sm', $sContent, $aMatches) != 0) {
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
        $aRawResult = $this->_oShell->exec($sCmd);
        $aLocalJobLogPaths = array();
        foreach ($aRawResult as $iIdx => $sS3JobLogPath) {
            $sLocalJobLogPath = '/tmp' . '/php-emr_' . md5(time().rand()) . '_job' . ($iIdx+1);
            $sCmd = "s3cmd get '$sS3JobLogPath' '$sLocalJobLogPath'";
            $this->_oShell->exec($sCmd);
            $aLocalJobLogPaths[] = $sLocalJobLogPath;
        }

        $sGnuplotData = '/tmp/toto_result.csv';
        if (count($aLocalJobLogPaths) > 0) {
            list($iMaxTs, $iMaxNbTasks) = $this->_extractHistory($aLocalJobLogPaths, $sGnuplotData);
        }

        return array($sSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData);
    }

    private function _extractHistory (array $aLocalJobLogPaths, $sDestPath)
    {
        $aMapStartTime = array(-1 => PHP_INT_MAX);
        $aMapEndTime = array(-1 => 0);
        $aReduceStartTime = array(-1 => PHP_INT_MAX);
        $aReduceShuffleTime = array(-1 => 0);
        $aReduceSortTime = array(-1 => 0);
        $aReduceEndTime = array(-1 => 0);
        $aReduceBytes = array();

        foreach ($aLocalJobLogPaths as $sSrcPath) {
            $rHandle = fopen($sSrcPath, 'r');
            while (($sLine = fgets($rHandle, 10000)) !== false) {
                $aWords = explode(' ', $sLine, 2);
                $sEvent = $aWords[0];
                if ($sEvent == 'MapAttempt') {
                    $aAttrs = $this->_parseAttributes($aWords[1]);
                    if (isset($aAttrs['START_TIME'])) {
                        $aMapStartTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['START_TIME']);
                    } else if (isset($aAttrs['FINISH_TIME'])) {
                        $aMapEndTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['FINISH_TIME']);
                    }
                } else if ($sEvent == 'ReduceAttempt') {
                    $aAttrs = $this->_parseAttributes($aWords[1]);
                    if (isset($aAttrs['START_TIME'])) {
                        $aReduceStartTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['START_TIME']);
                    } else if (
                        isset($aAttrs['SHUFFLE_FINISHED'])
                        && isset($aAttrs['SORT_FINISHED'])
                        && isset($aAttrs['FINISH_TIME'])
                    ) {
                        $aReduceShuffleTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['SHUFFLE_FINISHED']);
                        $aReduceSortTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['SORT_FINISHED']);
                        $aReduceEndTime[$aAttrs['TASKID']] = $this->_timestampToInt($aAttrs['FINISH_TIME']);
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
            $iMaxNbTasks = max(max($aRunningMaps), max($aShufflingReduces), max($aSortingReduces), max($aRunningReduces));
        }
        return array($iMaxTs, $iMaxNbTasks);
    }

    // 13 au lieu de 10 char
    private function _timestampToInt ($sTs)
    {
        //     return round((int)substr($sTs, 0, -3) + ((int)substr($sTs, -3))/1000.0);
        return (int)substr($sTs, 0, -3);
    }

    private function _parseAttributes ($sAttributes) {
        preg_match_all('/([^=]++)="([^"]*)" */m', $sAttributes, $aMatches, PREG_PATTERN_ORDER);
        $aAttributes = array_combine($aMatches[1], $aMatches[2]);
        return $aAttributes;
    }

    public function getHadoopInputFiles ($sJobFlowID, array $aJob)
    {
        $sTmpDirname = '/tmp' . '/php-emr_' . md5(time().rand());
        $sLogURI = str_replace('s3n://', 's3://', $aJob['LogUri']);
        $sLogURITasks = $sLogURI . "$sJobFlowID/task-attempts";
        $sSyncCmd = "s3cmd sync"
              . " --check-md5 --no-progress --no-delete-removed --exclude='*'"
              . " --rinclude='_m(_[0-9]+)+(\.cleanup)?/syslog'"
              . " '$sLogURITasks' '$sTmpDirname'";

        $sCmd = "mkdir '$sTmpDirname' && $sSyncCmd --dry-run | grep download | wc -l";
        $aResult = $this->_oShell->exec($sCmd);
        $iNbS3LogFilesToDownload = array_pop($aResult);
        $this->_oLogger->log(LogLevel::INFO, "{C.comment}Nb of S3 files to download: $iNbS3LogFilesToDownload");

        $this->_oShell->exec($sSyncCmd);

        $sBucketURI = substr($sLogURITasks, 0, strpos($sLogURITasks, '/', 5)-1);
        $sCmd = "grep -rP --color=never --only-matching --no-filename \"(?<=Opening '){$sBucketURI}[^']+(?=' for reading)\" '$sTmpDirname' | sort | uniq";
        $aInputFiles = $this->_oShell->exec($sCmd);
        return $aInputFiles;
    }
}
