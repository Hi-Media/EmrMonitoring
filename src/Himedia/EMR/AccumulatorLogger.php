<?php

namespace Himedia\EMR;

use GAubry\Logger\AbstractLogger;
use Psr\Log\LogLevel;

/**
 * PSR-3 Logger used to store message when JSON output is required.
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
class AccumulatorLogger extends AbstractLogger
{
    /**
     * Collection of logged messages.
     *
     * Structure of each message:
     * <code>
     * array(
     *     'level'   => a Psr\Log\LogLevel constant,
     *     'message' => content of the message
     * )
     * </code>
     *
     * @var array
     */
    private $aAccumulator;

    /**
     * Constructor.
     *
     * @param string $sMinMsgLevel threshold required to log message, must be defined in \Psr\Log\LogLevel
     * @throws \Psr\Log\InvalidArgumentException if calling this method with a level not defined in \Psr\Log\LogLevel
     */
    public function __construct ($sMinMsgLevel = LogLevel::DEBUG)
    {
        parent::__construct($sMinMsgLevel);
        $this->aAccumulator = array();
    }

    /**
     * Logs with an arbitrary level.
     *
     * @param string $sMsgLevel message level, must be defined in \Psr\Log\LogLevel
     * @param string $sMessage message with placeholders
     * @param array $aContext context array
     * @throws \Psr\Log\InvalidArgumentException if calling this method with a level not defined in \Psr\Log\LogLevel
     * @return null|void
     */
    public function log ($sMsgLevel, $sMessage, array $aContext = array())
    {
        $this->checkMsgLevel($sMsgLevel);
        if (self::$aIntLevels[$sMsgLevel] >= $this->iMinMsgLevel) {
            $this->aAccumulator[] = array(
                'level' => $sMsgLevel,
                'message' => $this->interpolateContext($sMessage, $aContext)
            );
        }
    }

    /**
     * Returns all logged messages.
     *
     * Structure of each message:
     * <code>
     * array(
     *     'level'   => a Psr\Log\LogLevel constant,
     *     'message' => content of the message
     * )
     * </code>
     *
     * @return array all logged messages.
     */
    public function getAccumulatedMessages ()
    {
        return $this->aAccumulator;
    }
}
