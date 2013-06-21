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
        'pricing_emr_json_url' => 'http://aws.amazon.com/elasticmapreduce/pricing/pricing-emr.json',
    );

    /**
     * List of EC2 instance types with mapping between JSON name and official name.
     * @var array
     */
    private static $aInstanceTypeMapping = array(
        'clustercompresi' => 'cc1',
        'clustercomputei' => 'cc1',
        'clustergpui' => 'cg1',
        'clustergpuresi' => 'cg1',
        'hicpuodi' => 'c1',
        'hicpuresi' => 'c1',
        'hiioodi' => 'hi1',
        'hiioresi' => 'hi1',
        'himemodi' => 'm2',
        'himemresi' => 'm2',
        'historeodi' => 'hs1',
        'stdodi' => 'm1',
        'stdresi' => 'm1',
        'uodi' => 't1',
        'uresi' => 't1',
        'secgenstdodi' => 'm3',
        'secgenstdresi' => 'm3'
    );

    /**
     * List of EC2 instance sizes with mapping between JSON name and official name.
     * @var array
     */
    private static $aInstanceSizeMapping = array(
        'lg' => 'large',
        'med' => 'medium',
        'sm' => 'small',
        'u' => 'micro',
        'xl' => 'xlarge',
        'xxl' => '2xlarge',
        'xxxxl' => '4xlarge',
        'xxxxxxxxl' => '8xlarge'
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
            $sUrl = $this->aConfig['pricing_emr_json_url'];
            $aData = json_decode(file_get_contents($sUrl), true);

            foreach ($aData['config']['regions'] as $iIdx => $aRegion) {
                $sRegion = self::$aRegionMapping[$aRegion['region']];
                $aRegion['region'] = $sRegion;
                $aData['config']['regions'][$sRegion] = $aRegion;
                unset($aData['config']['regions'][$iIdx]);

                $aNewRegion = &$aData['config']['regions'][$sRegion];
                foreach ($aNewRegion['instanceTypes'] as $iIdx => $aTypes) {
                    $sType = self::$aInstanceTypeMapping[strtolower($aTypes['type'])];
                    $aTypes['type'] = $sType;
                    $aNewRegion['instanceTypes'][$sType] = $aTypes;
                    unset($aNewRegion['instanceTypes'][$iIdx]);

                    $aNewTypes = &$aNewRegion['instanceTypes'][$sType];
                    foreach ($aNewTypes['sizes'] as $iIdx => $aSize) {
                        $sSize = self::$aInstanceSizeMapping[$aSize['size']];
                        $aSize['size'] = $sSize;
                        $aNewTypes['sizes'][$sSize] = $aSize;
                        unset($aNewTypes['sizes'][$iIdx]);

                        $aNewSize = &$aNewTypes['sizes'][$sSize];
                        foreach ($aNewSize['valueColumns'] as $iIdx => $aPrice) {
                            if ($aPrice['name'] == 'ec2') {
                                $aNewSize['valueColumns']['ec2'] = $aPrice;
                                unset($aNewSize['valueColumns'][$iIdx]);
                            }
                            if ($aPrice['name'] == 'emr') {
                                $aNewSize['valueColumns']['emr'] = $aPrice;
                                unset($aNewSize['valueColumns'][$iIdx]);
                            }
                        }
                    }
                }
            }
        }
        $this->aData = $aData;
    }

    /**
     * Returns EC2 plus the additional EMR price of an instance with the specified type, size and region.
     *
     * @param string $sRegion EC2 instance region
     * @param string $sInstanceType EC2 instance type
     * @param string $sSize EC2 instance size
     * @return float EC2 plus the additional EMR price of an instance with the specified type, size and region.
     */
    public function getUSDPrice ($sRegion, $sInstanceType, $sSize)
    {
        if (isset($this->aData['config']['regions'][$sRegion]['instanceTypes'][$sInstanceType]['sizes'][$sSize])) {
            $aData = $this->aData['config']['regions'][$sRegion]['instanceTypes'][$sInstanceType]['sizes'][$sSize];
            return $aData['valueColumns']['ec2']['prices']['USD'] + $aData['valueColumns']['emr']['prices']['USD'];
        } else {
            return 0;
        }
    }
}
