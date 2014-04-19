<?php

namespace Himedia\EMR;

use GAubry\Helpers\Helpers;

/**
 * Extract Amazon Elastic MapReduce Pricing from JSON stream used by http://aws.amazon.com/elasticmapreduce/pricing/.
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
class EMRInstancePrices
{
    /**
     * Default configuration.
     * Contains only one key specifying the JSON stream URL used by http://aws.amazon.com/elasticmapreduce/pricing/.
     * @var array
     */
    private static $aDefaultConfig = array(
        'pricing_emr_json_url' => 'https://a0.awsstatic.com/pricing/1/emr/pricing-emr.min.js',
    );

    /**
     * List of EC2 regions with mapping between JSON name and official name.
     * @var array
     */
    private static $aRegionMapping = array(
        'apac-sin' => 'ap-southeast-1',
        'apac-syd' => 'ap-southeast-2',
        'apac-tokyo' => 'ap-northeast-1',
        'ap-southeast-1' => 'ap-southeast-1',
        'ap-southeast-2' => 'ap-southeast-2',
        'ap-northeast-1' => 'ap-northeast-1',
        'eu-ireland' => 'eu-west-1',
        'eu-west-1' => 'eu-west-1',
        'sa-east-1' => 'sa-east-1',
        'us-east' => 'us-east-1',
        'us-east-1' => 'us-east-1',
        'us-west' => 'us-west-1',
        'us-west-1' => 'us-west-1',
        'us-west-2' => 'us-west-2'
    );

    /**
     * Current configuration.
     * @var array
     * @see $aDefaultConfig
     */
    private $aConfig;

    /**
     * Data extracted from JSON stream, then normalized.
     * @var array
     * @see resources/normalized-princing-emr-json-decoded.log
     */
    private $aData;

    /**
     * Constructor.
     *
     * @param array $aConfig current configuration
     */
    public function __construct(array $aConfig = array())
    {
        $this->aConfig = Helpers::arrayMergeRecursiveDistinct(self::$aDefaultConfig, $aConfig);
        $this->aData = array();
        $this->loadData();
    }

    /**
     * Extracts and normalizes data from JSON stream.
     * Called by constructor.
     * @see $aData
     * @see resources/raw-princing-emr.json
     */
    private function loadData ()
    {
        if (count($this->aData) == 0) {
            $sURL = $this->aConfig['pricing_emr_json_url'];
            $sContent = file_get_contents($sURL);
            if (preg_match('/callback\((.*)\);$/ms', $sContent, $aMatches) !==1) {
                throw new \RuntimeException("Content of '$sURL' not handled: $sContent");
            }
            $sBadJSON = $aMatches[1];
            $sJSON = preg_replace('/(?<!")(\w+):/', '"$1":', $sBadJSON);
            $aData = json_decode($sJSON, true);

            foreach ($aData['config']['regions'] as $idxRegion => $aRegion) {
                $sRegion = self::$aRegionMapping[$aRegion['region']];
                $aRegion['region'] = $sRegion;
                $aData['config']['regions'][$sRegion] = $aRegion;
                unset($aData['config']['regions'][$idxRegion]);

                $aNewRegion = &$aData['config']['regions'][$sRegion];
                foreach ($aNewRegion['instanceTypes'] as $idxType => $aType) {

                    foreach ($aType['sizes'] as $idxSize => $aSize) {
                        if (preg_match('/^([^.]+)\.([^.]+)$/', $aSize['size'], $aMatches) !== 1) {
                            $sMsg = "Size format not handled: '" . $aSize['size']
                                  . "'. Region: " . print_r($aRegion, true);
                            throw new \RuntimeException($sMsg);
                        }
                        $aType['type'] = $aMatches[1];
                        $sSize = $aMatches[2];
                        $aSize['size'] = $sSize;
                        $aType['sizes'][$sSize] = $aSize;
                        unset($aType['sizes'][$idxSize]);

                        $aNewSize = &$aType['sizes'][$sSize];
                        foreach ($aNewSize['valueColumns'] as $idxPrice => $aPrice) {
                            if ($aPrice['name'] == 'ec2') {
                                $aNewSize['valueColumns']['ec2'] = $aPrice;
                                unset($aNewSize['valueColumns'][$idxPrice]);
                            }
                            if ($aPrice['name'] == 'emr') {
                                $aNewSize['valueColumns']['emr'] = $aPrice;
                                unset($aNewSize['valueColumns'][$idxPrice]);
                            }
                        }
                        $aNewRegion['instanceTypes'][$aType['type']] = $aType;
                    }
                    unset($aNewRegion['instanceTypes'][$idxType]);
                }
            }
            $this->aData = $aData;
        }
    }

    /**
     * Returns both EC2 and additional EMR price of an instance with the specified type, size and region.
     *
     * @param string $sRegion EC2 instance region
     * @param string $sInstanceType EC2 instance type
     * @param string $sSize EC2 instance size
     * @return array both EC2 and additional EMR price (float) of an instance with the specified type, size and region.
     */
    public function getUSDPrice ($sRegion, $sInstanceType, $sSize)
    {
        if (isset($this->aData['config']['regions'][$sRegion]['instanceTypes'][$sInstanceType]['sizes'][$sSize])) {
            $aData = $this->aData['config']['regions'][$sRegion]['instanceTypes'][$sInstanceType]['sizes'][$sSize];
            $fEC2Price = (float)$aData['valueColumns']['ec2']['prices']['USD'];
            $fEMRPrice = (float)$aData['valueColumns']['emr']['prices']['USD'];
        } else {
            $fEC2Price = 0;
            $fEMRPrice = 0;
        }
        return array($fEC2Price, $fEMRPrice);
    }
}
