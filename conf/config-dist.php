<?php

$sRootDir = realpath(__DIR__ . '/..');
$aDirs = array(
    'root_dir'   => $sRootDir,
    'conf_dir'   => $sRootDir . '/conf',
    'lib_dir'    => $sRootDir . '/lib',
    'src_dir'    => $sRootDir . '/src',
    'class_dir'  => $sRootDir . '/src/class',
    'inc_dir'    => $sRootDir . '/src/inc',
    'vendor_dir' => $sRootDir . '/vendor'
);

$aConfig = $aDirs + array(
    'Himedia\EMR' => array(
        'emr_cli_bin' => '/usr/local/lib/elastic-mapreduce-cli/elastic-mapreduce',
        'ec2_api_tools_dir' => '/usr/local/lib/ec2-api-tools-1.6.7.2',
        'aws_access_key' => '…',
        'aws_secret_key' => '…',
        'ssh_options' => '-o ServerAliveInterval=10 -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes',
        'default_ssh_tunnel_port' => 12345
    ),
    'GAubry\ErrorHandler' => array(
        'display_errors' => true,
        'error_log_path' => '/var/log/emr-monitoring/emr-monitoring.error.log',
        'error_level' => -1,
        'auth_error_suppr_op' => false
    ),
    'GAubry\Logger' => array(
        'colors' => array(
            'job'           => "\033[1;34m",
            'section'       => "\033[1;37m",
            'subsection'    => "\033[1;33m",
            'subsubsection' => "\033[1;35m",
            'comment'       => "\033[1;30m",
            'debug'         => "\033[0;30m",
            'ok'            => "\033[1;32m",
            'discreet_ok'   => "\033[0;32m",
            'running'       => "\033[1;36m",
            'warm_up'       => "\033[0;36m",
            'warning'       => "\033[0;33m",
            'error'         => "\033[1;31m",
            'raw_error'     => "\033[0;31m",
            'info'          => "\033[0;37m",
            'price'         => "\033[0;35m",
            'help_cmd'      => "\033[0;33m",
            'help_param'    => "\033[1;33m"
        ),
        'tabulation' => "\033[0;30m┆\033[0m   ",
        'indent_tag' => '+++',
        'unindent_tag' => '---',
    )
);

return $aConfig;
