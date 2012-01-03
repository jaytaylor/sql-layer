/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.test;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import org.junit.Test;

import java.io.IOException;

/*
 * Create a YAML test file for testing time intervals.
 */
public class IntervalMatrixCreator {

    @Test
    public void create() throws IOException {
        main(new String[0]);
    }

    static final String timeZero = ("00:00:00");

    private static final String[] times = {
        "00:00:00",
        "00:00:01",
        "00:00:59",
        "00:00:22",
        "00:01:00",
        "00:59:00",
        "00:59:59",
        "00:37:14",
        "01:00:00",
        "23:00:00",
        "23:59:59",
        "17:07:23"
    };

    private static final DateTimeFormatter timeFormatter =
        DateTimeFormat.forPattern("HH:mm:ss");

    private static final String[] timeIntervals = {
        "00:00:01",
        "00:00:59",
        "00:00:13",
        "00:01:00",
        "00:01:59",
        "00:59:00",
        "00:59:59",
        "00:17:53",
        "01:00:00",
        "01:59:59",
        "23:00:00",
        "23:59:59",
        "14:22:41"
    };

    private static final PeriodFormatter timeIntervalFormatter =
        new PeriodFormatterBuilder()
        .appendHours()
        .appendSeparator(":")
        .appendMinutes()
        .appendSeparator(":")
        .appendSeconds()
        .toFormatter();

    private static final String dateZero = "2000-01-01";

    private static final DateTime zero = new DateTime(dateZero);

    private static final String[] days = { "01", "31", "17" };

    private static final String[] months = { "01", "11", "06" };

    private static final String[] years = { "2000", "1999", "1961" };

    private static final DateTimeFormatter dateFormatter =
        DateTimeFormat.forPattern("YYYY-MM-dd");

    private static final DateTimeFormatter timestampFormatter =
        DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");

    private static final String[] dayIntervals = { "1", "31", "17" };

    private static final String[] weekIntervals = { "1", "13", "51" };

    private static final String[] monthIntervals = { "1", "5", "11" };

    private static final String[] yearIntervals = { "1", "33", "127" };

    public static void main(String[] args) throws IOException {
        StringBuilder sb = new StringBuilder();
        createTimeTable(sb);
        insertTimes(sb);
        selectTimes(sb);
        createDateTable(sb);
        insertDates(sb);
        selectDates(sb);
        /* Interval computations on timestamps are not currently working. */
        // createTimestampTable(sb);
        // insertTimestamps(sb);
        // selectTimestamps(sb);
        sb.append("...\n");
        System.out.println(sb);
    }

    private static void createTimeTable(StringBuilder out) {
        out.append("---\n");
        out.append("- CreateTable: times (ind int, t1 time, t2 time, t3 time," +
                   " t4 time)\n");
    }

    /* ind, t1 (time), t2 (interval), t3 (t1 + t2), t4 (t1 - t2) */
    private static void insertTimes(StringBuilder out) throws IOException {
        int index = 1;
        DateTime zero = new DateTime("T" + timeZero);
        for (String timeString : times) {
            DateTime time = new DateTime("T" + timeString);
            for (String intervalString : timeIntervals) {
                Period interval =
                    timeIntervalFormatter.parsePeriod(intervalString);
                out.append("---\n");
                out.append("- Statement: INSERT INTO times\n");
                out.append("    VALUES (");
                out.append(index++).append(", '");
                timeFormatter.printTo(out, time);
                out.append("', '");
                timeFormatter.printTo(out, zero.plus(interval));
                out.append("', '");
                timeFormatter.printTo(out, time.plus(interval));
                out.append("', '");
                timeFormatter.printTo(out, time.minus(interval));
                out.append("')\n");
            }
        }
    }

    private static void selectTimes(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, t1 + (t2 - TIME('00:00:00'))" +
                   " FROM times\n");
        out.append("    WHERE t1 + (t2 - TIME('00:00:00')) <> t3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, t1 - (t2 - TIME('00:00:00'))" +
                   " FROM times\n");
        out.append("    WHERE t1 - (t2 - TIME('00:00:00')) <> t4\n");
        out.append("- row_count: 0\n");
    }

    private static void createDateTable(StringBuilder out) {
        out.append("---\n");
        out.append("- CreateTable: dates (ind int, d1 date, d2 date," +
                   " d3 date, d4 date)\n");
    }

    private static void insertDates(StringBuilder out) throws IOException {
        int index = 1;
        for (String day : days) {
            index = insertDates(
                out,
                new DateTime(years[0] + "-" + months[0] + "-" + day),
                index);
        }
        for (String month : months) {
            index = insertDates(
                out,
                new DateTime(years[0] + "-" + month + "-" + days[0]),
                index);
        }
        for (String year : years) {
            index = insertDates(
                out,
                new DateTime(year + "-" + months[0] + "-" + days[0]),
                index);
        }
    }

    private static int insertDates(StringBuilder out,
                                   DateTime date,
                                   int index)
            throws IOException
    {
        for (String interval : dayIntervals) {
            insertOneDate(out, date,
                          zero.plusDays(Integer.parseInt(interval)),
                          index++);
        }
        for (String interval : weekIntervals) {
            insertOneDate(out, date,
                          zero.plusWeeks(Integer.parseInt(interval)),
                          index++);
        }
        for (String interval : monthIntervals) {
            insertOneDate(out, date,
                          zero.plusMonths(Integer.parseInt(interval)),
                          index++);
            }
        for (String interval : yearIntervals) {
            insertOneDate(out, date,
                          zero.plusMonths(Integer.parseInt(interval)),
                          index++);
        }
        return index;
    }

    private static void insertOneDate(StringBuilder out, DateTime date,
                                      DateTime intervalEnd, int index)
            throws IOException
    {
        Duration interval = new Duration(zero, intervalEnd);
        out.append("---\n");
        out.append("- Statement: INSERT INTO dates\n");
        out.append("    VALUES (");
        out.append(index).append(", '");
        dateFormatter.printTo(out, date);
        out.append("', '");
        dateFormatter.printTo(out, zero.plus(interval));
        out.append("', '");
        dateFormatter.printTo(out, date.plus(interval));
        out.append("', '");
        dateFormatter.printTo(out, date.minus(interval));
        out.append("')\n");
    }

    private static void selectDates(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, d1 + (d2 - DATE('2000-01-01'))" +
                   " FROM dates\n");
        out.append("    WHERE d1 + (d2 - DATE('2000-01-01')) <> d3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, d1 - (d2 - DATE('2000-01-01'))" +
                   " FROM dates\n");
        out.append("    WHERE d1 - (d2 - DATE('2000-01-01')) <> d4\n");
        out.append("- row_count: 0\n");
    }

    private static void createTimestampTable(StringBuilder out) {
        out.append("---\n");
        out.append("- CreateTable: timestamps (ind int, ts1 timestamp," +
                   " ts2 timestamp,\n" +
                   "    ts3 timestamp, ts4 timestamp)\n");
    }

    private static void insertTimestamps(StringBuilder out)
            throws IOException
    {
        int index = 1;
        for (String day : days) {
            String timeString = times[(index * 7) % times.length];
            String timeIntervalString =
                timeIntervals[(index * 11) % timeIntervals.length];
            index = insertTimestamps(
                out,
                new DateTime(years[0] + "-" + months[0] + "-" + day +
                             "T" + timeString),
                timeIntervalString,
                index);
        }
        for (String month : months) {
            String timeString = times[(index * 13) % times.length];
            String timeIntervalString =
                timeIntervals[(index * 17) % timeIntervals.length];
            index = insertTimestamps(
                out,
                new DateTime(years[0] + "-" + month + "-" + days[0] +
                             "T" + timeString),
                timeIntervalString,
                index);
        }
        for (String year : years) {
            String timeString = times[(index * 19) % times.length];
            String timeIntervalString =
                timeIntervals[(index * 23) % timeIntervals.length];
            index = insertTimestamps(
                out,
                new DateTime(year + "-" + months[0] + "-" + days[0] +
                             "T" + timeString),
                timeIntervalString,
                index);
        }
    }

    private static int insertTimestamps(StringBuilder out,
                                        DateTime date,
                                        String timeIntervalString,
                                        int index)
            throws IOException
    {
        DateTime base =
            zero.plus(timeIntervalFormatter.parsePeriod(timeIntervalString));
        for (String interval : dayIntervals) {
            insertOneTimestamp(out, date,
                               base.plusDays(Integer.parseInt(interval)),
                               index++);
        }
        for (String interval : weekIntervals) {
            insertOneTimestamp(out, date,
                               base.plusWeeks(Integer.parseInt(interval)),
                               index++);
        }
        for (String interval : monthIntervals) {
            insertOneTimestamp(out, date,
                               base.plusMonths(Integer.parseInt(interval)),
                               index++);
            }
        for (String interval : yearIntervals) {
            insertOneTimestamp(out, date,
                               base.plusMonths(Integer.parseInt(interval)),
                               index++);
        }
        return index;
    }

    private static void insertOneTimestamp(StringBuilder out, DateTime date,
                                           DateTime intervalEnd, int index)
            throws IOException
    {
        Duration interval = new Duration(zero, intervalEnd);
        out.append("---\n");
        out.append("- Statement: INSERT INTO timestamps\n");
        out.append("    VALUES (");
        out.append(index).append(", '");
        timestampFormatter.printTo(out, date);
        out.append("', '");
        timestampFormatter.printTo(out, zero.plus(interval));
        out.append("',\n   '");
        timestampFormatter.printTo(out, date.plus(interval));
        out.append("', '");
        timestampFormatter.printTo(out, date.minus(interval));
        out.append("')\n");
    }

    private static void selectTimestamps(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, ts1 + (ts2 - DATE('2000-01-01'))" +
                   " FROM timestamps\n");
        out.append("    WHERE ts1 + (ts1 - TIMESTAMP('2000-01-01')) <> ts3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, ts1 - (ts2 - DATE('2000-01-01'))" +
                   " FROM timestamps\n");
        out.append("    WHERE ts1 - (ts2 - DATE('2000-01-01')) <> ts4\n");
        out.append("- row_count: 0\n");
    }
}
