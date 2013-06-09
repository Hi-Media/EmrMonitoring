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

    public function __construct(array $aConfig, $bDebugMode)
    {
        $this->aConfig = $aConfig;
        $this->aConfig['GAubry\Logger']['min_message_level'] = ($bDebugMode ? LogLevel::DEBUG : LogLevel::INFO);
        $oLogger = new ColoredIndentedLogger($this->aConfig['GAubry\Logger']);
        $oEMRInstancePrices = new EMRInstancePrices();
        $this->oMonitoring = new Monitoring($oLogger, $oEMRInstancePrices, $this->aConfig['Himedia\EMR']);
        $this->oRendering = new Rendering($oLogger);
    }

    public function run (array $aParameters)
    {
        if (! empty($aParameters['error']) || isset($aParameters['help'])) {
            $this->oRendering->displayHelp($aParameters['error']);
        } elseif (! empty($aParameters['list-all-jobflows'])) {
            $this->displayAllJobs();
        } elseif (! empty($aParameters['jobflow-id'])) {
            if (! isset($aParameters['ssh-tunnel-port'])) {
                $aParameters['ssh-tunnel-port'] = $this->aConfig['Himedia\EMR']['default_ssh_tunnel_port'];
            }
            if (isset($aParameters['list-input-files'])) {
                $this->displayHadoopInputFiles($aParameters);
            } else {
                $this->displayJobFlow($aParameters);
            }
        } else {
            $this->oRendering->displayHelp();
        }
    }

    private function displayAllJobs ()
    {
        $aAllJobs = $this->oMonitoring->getAllJobs();
        $this->oRendering->displayAllJobs($aAllJobs);
    }

    private function displayHadoopInputFiles(array $aParameters)
    {
        $sJobflowId = $aParameters['jobflow-id'];
        $iSSHTunnelPort = (int)$aParameters['ssh-tunnel-port'];
        $aJob = $this->oMonitoring->getJobFlow($sJobflowId, $iSSHTunnelPort);
        $aInputFiles = $this->oMonitoring->getHadoopInputFiles($sJobflowId, $aJob);
        $this->oRendering->displayHadoopInputFiles($aInputFiles);
    }

    private function displayJobFlow (array $aParameters)
    {
        $sJobflowId = $aParameters['jobflow-id'];
        $iSSHTunnelPort = (int)$aParameters['ssh-tunnel-port'];
        $aJob = $this->oMonitoring->getJobFlow($sJobflowId, $iSSHTunnelPort);
        $this->oRendering->displayJobName($aJob['Name']);
        $this->oRendering->displayJobGeneralStatus($aJob);
        $this->oRendering->displayJobInstances($aJob);
        $this->oRendering->displayJobSteps($aJob);

        list($sRawSummary, $aErrorMsg, $aS3LogSteps, $iMaxTs, $iMaxNbTasks, $sGnuplotData)
            = $this->oMonitoring->getLogSummary($sJobflowId, $aJob);
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
