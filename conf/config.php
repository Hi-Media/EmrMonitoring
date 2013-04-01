<?php

/**
 * Répertoire racine de l'application.
 * @var string
 */
define('ROOT_DIR', realpath(__DIR__ . '/../'));
define('VENDOR_DIR', ROOT_DIR . '/vendor');

// return array(
//     'Main' => array(
//         // Répertoire des fichiers de configuration de l'application elle-même.
//         'conf_dir' => ROOT_DIR . '/conf',
//     ),

//     'GAubry\Shell' => array(
//         // Nombre maximal de processus lancés en parallèle par parallelize.inc.sh.
//         'parallelization_max_nb_processes' => 10,

//         // Chemin vers le shell bash.
//         'bash_path' => '/bin/bash',

//         // Répertoire des bibliothèques utilisées par l'application.
//         'lib_dir' => ROOT_DIR . '/lib',

//         // Nombre de secondes avant timeout lors d'une connexion SSH.
//         'ssh_connection_timeout' => 10,

//         // Chemin du répertoire temporaire système utilisable par l'application.
//         'tmp_dir' => '/tmp',

//         // Nombre maximal d'exécutions shell rsync en parallèle.
//         // Prioritaire sur 'parallelization_max_nb_processes'.
//         'rsync_max_nb_processes' => 5,

//         'tests_resources_dir' => ROOT_DIR . '/tests/resources',
//     ),
// );

// DIRECTORIES
define('EMR_ROOT_DIR', realpath(__DIR__ . '/..'));
define('EMR_CONF_DIR', EMR_ROOT_DIR . '/conf');
define('EMR_LIB_DIR', EMR_ROOT_DIR . '/lib');
define('EMR_SRC_DIR', EMR_ROOT_DIR . '/src');
define('EMR_CLASS_DIR', EMR_SRC_DIR . '/class');
define('EMR_INC_DIR', EMR_SRC_DIR . '/inc');
define('EMR_TMP_DIR', '/tmp');

// Logs
define('EMR_LOG_DIR', '/var/log/php-emr');
define('EMR_DEBUG_FILENAME', EMR_LOG_DIR . '/php-emr.error.log');
define('EMR_LOG_TABULATION', "\033[0;30m┆\033[0m   ");

// Error handler
define('EMR_DISPLAY_ERRORS', true);
define('EMR_ERROR_LOG_PATH', EMR_DEBUG_FILENAME);
define('EMR_ERROR_LEVEL', -1);
define('EMR_AUTH_ERROR_SUPPR_OP', false);

define('EMR_ELASTIC_MAPREDUCE_CLI', '~/elastic-mapreduce-cli/elastic-mapreduce-ruby/elastic-mapreduce');
define('EMR_DEFAULT_SSH_TUNNEL_PORT', 12345);

$GLOBALS['CUI_COLORS'] = array(
    'job'          => "\033[1;34m",
    'section'      => "\033[1;37m",
    'subsection'   => "\033[1;33m",
    'subsubsection' => "\033[1;35m",
    'comment'      => "\033[1;30m",
    'debug'        => "\033[0;30m",
    'ok'           => "\033[1;32m",
    'discreet_ok'  => "\033[0;32m",
    'running'      => "\033[1;36m",
    'warm_up'      => "\033[0;36m",
    'warning'      => "\033[0;33m",
    'error'        => "\033[1;31m",
    'raw_error'    => "\033[0;31m",
    'info'    => "\033[0;37m",
);