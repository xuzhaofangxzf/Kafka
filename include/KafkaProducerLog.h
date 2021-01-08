#ifndef __QBUSMAGAGERLOG_H_
#define __QBUSMAGAGERLOG_H_

#include <stdio.h>

typedef enum {
    LOG_ERROR, LOG_WARNING, LOG_DUMP, LOG_NOTICE, LOG_INFO, LOG_DEBUG
} LOG_LEVEL;

typedef enum {
    LOG_WEEK_DAY, LOG_MONTH_DAY, LOG_YEAR_DAY, LOG_YEAR_DAY_HOUR
} LOG_ROLL;

void log_init(const char *logfile, LOG_ROLL = LOG_WEEK_DAY, bool append = false);
void log_init(const char *logfile, LOG_ROLL = LOG_WEEK_DAY, LOG_LEVEL = LOG_NOTICE, bool append = false, bool notice_detach = false, bool dump_detach = false, bool flush_each_line = true, int max_reserve_days = 7);
void log_close();
void log(int level, const char *fmt, ...);

// refer to https://gcc.gnu.org/onlinedocs/cpp/Variadic-Macros.html
#define LOG(level, fmt, ...)                                   \
    do {                                                       \
        if (0 == (unsigned long)(void*)fmt) {                  \
            printf(fmt, ##__VA_ARGS__); /* never reach here */ \
        }                                                      \
        log(level, fmt, ##__VA_ARGS__);                        \
    } while (0)

#endif //end of LOG_H_
