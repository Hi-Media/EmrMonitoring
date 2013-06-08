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

namespace Himedia\EMR;

use GAubry\Logger\ColoredIndentedLogger;
use Himedia\EMR\EMRInstancePrices;
use Himedia\EMR\Monitoring;
use Himedia\EMR\Rendering;
use Psr\Log\LogLevel;

class Controller
{

    private $bListInputFiles;
    private $sLogLevel;
    private $sJobFlowID;
    private $sSSHTunnelPort;

    private $oMonitoring;
    private $oRendering;

    public function __construct(array $aParameters, array $aConfig)
    {
        $this->aConfig = $aConfig;
        $this->extractParameters($aParameters);

        $this->aConfig['GAubry\Logger']['min_message_level'] = $this->sLogLevel;
        $oLogger = new ColoredIndentedLogger($this->aConfig['GAubry\Logger']);
        $oEMRInstancePrices = new EMRInstancePrices();
        $this->oMonitoring = new Monitoring($oLogger, $oEMRInstancePrices, $this->aConfig['Himedia\EMR']);
        $this->oRendering = new Rendering($oLogger);
    }

    private function extractParameters (array $aParameters)
    {
        $this->bListInputFiles = false;
        $this->sLogLevel = LogLevel::INFO;
        foreach ($aParameters as $iKey => $sValue) {
            if ($sValue == '--debug' || $sValue == '-d') {
                $this->sLogLevel = LogLevel::DEBUG;
                array_splice($aParameters, $iKey, 1, array());
            }
            if ($sValue == '--list-input-files') {
                $this->bListInputFiles = true;
                array_splice($aParameters, $iKey, 1, array());
            }
        }
        $this->sJobFlowID = (isset($aParameters[1]) ? $aParameters[1] : '');
        if (isset($aParameters[2])) {
            $this->sSSHTunnelPort = (int)$aParameters[2];
        } else {
            $this->sSSHTunnelPort = $this->aConfig['Himedia\EMR']['default_ssh_tunnel_port'];
        }
    }

    public function run ()
    {
        if (empty($this->sJobFlowID)) {
            $this->displayAllJobs();
        } elseif ($this->bListInputFiles) {
            $this->displayHadoopInputFiles();
        } else {
            $this->displayJobFlow();
        }
    }

    private function displayAllJobs ()
    {
        $this->oRendering->displayHelp();
        $aAllJobs = $this->oMonitoring->getAllJobs();
        $this->oRendering->displayAllJobs($aAllJobs);
    }

    private function displayHadoopInputFiles()
    {
        $aJob = $this->oMonitoring->getJobFlow($this->sJobFlowID, $this->sSSHTunnelPort);
        $aInputFiles = $this->oMonitoring->getHadoopInputFiles($this->sJobFlowID, $aJob);
        $this->oRendering->displayHadoopInputFiles($aInputFiles);
    }

    private function displayJobFlow ()
    {
        $aJob = $this->oMonitoring->getJobFlow($this->sJobFlowID, $this->sSSHTunnelPort);
        $this->oRendering->displayJobName($aJob['Name']);
        $this->oRendering->displayJobGeneralStatus($aJob);
        $this->oRendering->displayJobInstances($aJob);
        $this->oRendering->displayJobSteps($aJob);

        list($sRawSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData)
            = $this->oMonitoring->getLogSummary($this->sJobFlowID, $aJob);
        $this->oRendering->displayJobSummary(
            $aJob,
            $sRawSummary,
            $aErrorMsg,
            $aS3LogSteps,
            $iMaxTs,
            $iMaxNbTasks,
            $sGnuplotData,
            $this->aConfig['inc_dir'] . '/plot.script'
        );
        echo PHP_EOL;
    }
}
