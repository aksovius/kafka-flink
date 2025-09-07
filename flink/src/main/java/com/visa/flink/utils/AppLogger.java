package com.visa.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application logger utility для централизованного логирования
 */
public class AppLogger {
    private static final Logger logger = LoggerFactory.getLogger(AppLogger.class);

    public static void info(String message, Object... args) {
        logger.info(message, args);
    }

    public static void info(String message, Throwable throwable) {
        logger.info(message, throwable);
    }

    public static void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    public static void debug(String message, Throwable throwable) {
        logger.debug(message, throwable);
    }

    public static void warn(String message, Object... args) {
        logger.warn(message, args);
    }

    public static void warn(String message, Throwable throwable) {
        logger.warn(message, throwable);
    }

    public static void error(String message, Object... args) {
        logger.error(message, args);
    }

    public static void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }
}
