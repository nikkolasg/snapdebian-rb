require 'logger'
$logger = Logger.new(STDOUT)
#$logger.datetime_format = '%Y-%m-%d %H:%M:%S'
$logger.datetime_format = '%H:%M:%S'
$logger.level = Logger::INFO


