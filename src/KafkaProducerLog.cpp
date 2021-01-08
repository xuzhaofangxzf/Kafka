#include <assert.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include "KafkaProducerLog.h"

char            g_logfile_name[1024];
FILE*           g_logfile          = stderr;
FILE*           g_notice           = stderr;
FILE*           g_dump             = stderr;
pthread_mutex_t g_log_mutex        = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t g_log_notice_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t g_log_dump_mutex   = PTHREAD_MUTEX_INITIALIZER;
LOG_ROLL        g_logroll          = LOG_WEEK_DAY;
LOG_LEVEL       g_loglevel         = LOG_NOTICE;
int             g_logday           = 0;
bool            g_logappend        = true;
bool            g_notice_detach    = false;
bool            g_dump_detach      = false;
bool            g_flush_each_line  = true;
int             g_max_reserve_days = 7;

static void delete_obsolete_logs(bool is_init = false) {
    if (LOG_YEAR_DAY != g_logroll && LOG_YEAR_DAY_HOUR != g_logroll) {
        return;
    }
    int max_count = 1;
    if (is_init) {
        if (LOG_YEAR_DAY == g_logroll) {
            max_count = 100;
        }
        else {
            max_count = 100*24;
        }
    }

    struct tm t;
    time_t curtime = time(NULL);
    curtime -= g_max_reserve_days * 86400;
    char logfile_name[1024] = {'\0'};
    for (int i = 0; i < max_count; ++i) {
        localtime_r(&curtime, &t);
        int logday = 0;
        if (LOG_YEAR_DAY == g_logroll) {
            logday = (t.tm_year + 1900) * 10000 + (t.tm_mon + 1) * 100 + t.tm_mday;
        }
        else {
            logday = (t.tm_year + 1900) * 1000000 + (t.tm_mon + 1) * 10000 + t.tm_mday * 100 + t.tm_hour;
        }

        sprintf(logfile_name, "%s.%d.log", g_logfile_name, logday);
        if (access(logfile_name, F_OK) != 0) {
            break;
        }
        remove(logfile_name);
        if (g_notice_detach) {
            sprintf(logfile_name, "%s.NOTICE.%d.log", g_logfile_name, logday);
            remove(logfile_name);
        }
        if (g_dump_detach) {
            sprintf(logfile_name, "%s.DUMP.%d.dat", g_logfile_name, logday);
            remove(logfile_name);
        }
        curtime -= 86400;
    }
}

void log_init(const char *logfile, LOG_ROLL logroll, bool append) {
    log_init(logfile, logroll, LOG_NOTICE, append, false, false, true);
}

void log_init(const char *logfile, LOG_ROLL logroll, LOG_LEVEL loglevel, bool append, bool notice_detach, bool dump_detach, bool flush_each_line, int max_reserve_days) {
    size_t logfilename_len = strlen(logfile);
    assert(logfile != NULL && logfilename_len < 1000);

    if (logfilename_len > 4 && 0 == strcasecmp(".log", logfile + logfilename_len - 4)) {
        strncpy(g_logfile_name, logfile, logfilename_len - 4);
        g_logfile_name[logfilename_len - 4] = '\0';
    } else {
        strcpy(g_logfile_name, logfile);
    }
    g_logroll = logroll;
    g_loglevel = loglevel;
    g_logappend = append;
    g_notice_detach = notice_detach;
    g_dump_detach = dump_detach;
    g_flush_each_line = flush_each_line;
    g_max_reserve_days = max_reserve_days;

    struct tm t;
    time_t curtime = time(NULL);
    assert(localtime_r(&curtime, &t));
    switch (g_logroll) {
    case LOG_WEEK_DAY:
        g_logday = t.tm_wday;
        break;
    case LOG_MONTH_DAY:
        g_logday = t.tm_mday;
        break;
    case LOG_YEAR_DAY:
        g_logday = (t.tm_year + 1900) * 10000 + (t.tm_mon + 1) * 100 + t.tm_mday;
        break;
    case LOG_YEAR_DAY_HOUR:
        g_logday = (t.tm_year + 1900) * 1000000 + (t.tm_mon + 1) * 10000 + t.tm_mday * 100 + t.tm_hour;
        break;
    }
    pthread_mutex_init(&g_log_mutex, NULL);
    pthread_mutex_init(&g_log_notice_mutex, NULL);
    pthread_mutex_init(&g_log_dump_mutex, NULL);
    char logfile_name[1024] = {'\0'};
    sprintf(logfile_name, "%s.%d.log", g_logfile_name, g_logday);
    g_logfile = fopen(logfile_name, (g_logappend) ? "a" : "w");
    assert(g_logfile != NULL);
    if (g_notice_detach) {
        sprintf(logfile_name, "%s.NOTICE.%d.log", g_logfile_name, g_logday);
        g_notice = fopen(logfile_name, (g_logappend) ? "a" : "w");
        assert(g_notice != NULL);
    }
    if (g_dump_detach) {
        sprintf(logfile_name, "%s.DUMP.%d.dat", g_logfile_name, g_logday);
        g_dump = fopen(logfile_name, (g_logappend) ? "a" : "w");
        assert(g_dump != NULL);
    }
    delete_obsolete_logs(true);
}

void log_close() {
    assert(g_logfile != NULL);
    fclose(g_logfile);
    if (g_notice_detach) {
        assert(g_notice != NULL);
        fclose(g_notice);
    }
    if (g_dump_detach) {
        assert(g_dump != NULL);
        fclose(g_dump);
    }
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_log_notice_mutex);
    pthread_mutex_destroy(&g_log_dump_mutex);
}

void log(int level, const char *fmt, ...) {
    if (level > g_loglevel)
        return;

    time_t cur_time = time(time_t(NULL));
    struct tm t;
    tm *p = localtime_r(&cur_time, &t);

    char ostr[256];
    snprintf(ostr, 256, "%4d-%02d-%02d %02d:%02d:%02d\t", p->tm_year + 1900, p->tm_mon + 1, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec);

    int logday = 0;
    switch (g_logroll) {
    case LOG_WEEK_DAY:
        logday = p->tm_wday;
        break;
    case LOG_MONTH_DAY:
        logday = p->tm_mday;
        break;
    case LOG_YEAR_DAY:
        logday = (p->tm_year + 1900) * 10000 + (p->tm_mon + 1) * 100 + p->tm_mday;
        break;
    case LOG_YEAR_DAY_HOUR:
        logday = (p->tm_year + 1900) * 1000000 + (p->tm_mon + 1) * 10000 + p->tm_mday * 100 + p->tm_hour;
        break;
    }

    if (logday > g_logday) {
        pthread_mutex_lock(&g_log_mutex);
        if (logday > g_logday) {
            delete_obsolete_logs(false);
            g_logday = logday;
            char logfile_name[1024] = {'\0'};
            if (g_logfile != NULL && g_logfile != stderr && g_logfile != stdout) {
                fclose(g_logfile);
                sprintf(logfile_name, "%s.%d.log", g_logfile_name, g_logday);
                g_logfile = fopen(logfile_name, "w");
                assert(g_logfile != NULL);
            }
            if (g_notice_detach && g_notice != NULL && g_notice != stderr && g_notice != stdout) {
                pthread_mutex_lock(&g_log_notice_mutex);
                fclose(g_notice);
                sprintf(logfile_name, "%s.NOTICE.%d.log", g_logfile_name, g_logday);
                g_notice = fopen(logfile_name, "w");
                assert(g_notice != NULL);
                pthread_mutex_unlock(&g_log_notice_mutex);
            }
            if (g_dump_detach && g_dump != NULL && g_dump != stderr && g_dump != stdout) {
                pthread_mutex_lock(&g_log_dump_mutex);
                fclose(g_dump);
                sprintf(logfile_name, "%s.DUMP.%d.dat", g_logfile_name, g_logday);
                g_dump = fopen(logfile_name, "w");
                assert(g_dump != NULL);
                pthread_mutex_unlock(&g_log_dump_mutex);
            }
        }
        pthread_mutex_unlock(&g_log_mutex);
    }

    FILE* logfile = NULL;
    if (g_notice_detach && LOG_NOTICE == level) {
        pthread_mutex_lock(&g_log_notice_mutex);
        logfile = (g_notice) ? g_notice: stderr;
    } else if (g_dump_detach && LOG_DUMP == level) {
        pthread_mutex_lock(&g_log_dump_mutex);
        logfile = (g_dump) ? g_dump: stderr;
    } else {
        pthread_mutex_lock(&g_log_mutex);
        logfile = (g_logfile) ? g_logfile : stderr;
    }

    switch (level) {
    case LOG_ERROR:
        fprintf(logfile, "%s", ostr);
        fprintf(logfile, "ERROR\t");
        break;
    case LOG_WARNING:
        fprintf(logfile, "%s", ostr);
        fprintf(logfile, "WARNING\t");
        break;
    case LOG_NOTICE:
        fprintf(logfile, "%s", ostr);
        fprintf(logfile, "NOTICE\t");
        break;
    case LOG_INFO:
        fprintf(logfile, "%s", ostr);
        fprintf(logfile, "INFO\t");
        break;
    case LOG_DEBUG:
        fprintf(logfile, "%s", ostr);
        fprintf(logfile, "DEBUG\t");
        break;
    }

    va_list list;
    va_start(list, fmt);
    vfprintf(logfile, fmt, list);
    va_end(list);
#ifndef BACKWARD_COMPATIBLE_LOG
    fprintf(logfile, "\n");
#endif
    if (g_flush_each_line) {
        fflush(logfile);
    }

    if (g_notice_detach && LOG_NOTICE == level) {
        pthread_mutex_unlock(&g_log_notice_mutex);
    } else if (g_dump_detach && LOG_DUMP == level) {
        pthread_mutex_unlock(&g_log_dump_mutex);
    } else {
        pthread_mutex_unlock(&g_log_mutex);
    }
}
