class Log4j(object):
    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(self.__class__.__name__)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log a warning.
        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
