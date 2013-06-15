#!/bin/bash
#
# Compute size in MiB of S3 objects of the following forms:
#   – s3://path/to/dir
#   – s3://path/to/{28+29+30}+s3://path/to/{01+02+03}
#
#
#
# Copyright (c) 2013 Hi-Media SA
# Copyright (c) 2013 Geoffroy Aubry <gaubry@hi-media.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
# for the specific language governing permissions and limitations under the License.
#

a="$1"
b="${a//+/,}"
c="${b//,s3:\/\// s3:\/\/}"
dirs="$(eval echo "$c")"

result="$(for dir in $dirs; do s3cmd du "$dir" & done; wait)"
echo "$result" | grep -E '^[0-9]+ ' | awk 'BEGIN {sum=0} {sum+=$1} END {printf("%.0f", sum/1024/1024)}'
