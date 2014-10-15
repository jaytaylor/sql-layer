/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.test;

import org.joda.time.DateTime;
import static org.joda.time.DateTimeZone.UTC;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Formatter;

/*
 * Create the contents of a YAML test file for testing time intervals.
 * Includes tests for year/month and day/hour/minute/second intervals and date,
 * time, datetime, and timestamp data types.
 *
 * <p>Specify this class explicitly as a unit test class to generate the file.
 */
public class IntervalMatrixCreator {

    // these time strings are picked in such a way that they couldn't possibly be a valid date string
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
        "17:27:23"
    };

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

    static final String timeZeroString = "00:00:00";

    static final DateTime timeZero = new DateTime("T" + timeZeroString, UTC);

    static final String dateZeroString = "2000-01-01";

    private static final DateTime dateZero = new DateTime(dateZeroString, UTC);

    private static final String[] days = { "01", "31", "17" };

    private static final String[] months = { "01", "11", "06" };

    private static final String[] years = { "2000", "1999", "1961" };

    private static final String[] dayIntervals = { "1", "31", "17", "42" };

    private static final String[] monthIntervals = { "1", "5", "11" };

    private static final String[] yearIntervals = { "1", "33", "127" };

    private static final DateTimeFormatter timeFormatter =
        DateTimeFormat.forPattern("HH:mm:ss");

    private static final PeriodFormatter timeIntervalFormatter =
        new PeriodFormatterBuilder()
        .appendHours()
        .appendSeparator(":")
        .appendMinutes()
        .appendSeparator(":")
        .appendSeconds()
        .toFormatter();

    private static final DateTimeFormatter dateFormatter =
        DateTimeFormat.forPattern("YYYY-MM-dd");

    private static final DateTimeFormatter dateTimeFormatter =
        DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");

    /** Generate the YAML file. */
    @Test
    public void create() throws IOException {
        main(new String[] { "foo.yaml" });
    }

    /**
     * Generate the YAML output, printing it in the file named by the first
     * argument or printing it to standard output if no argument is supplied.
     */
    public static void main(String[] args) throws IOException {
        StringBuilder sb = new StringBuilder();
        createTimeTable(sb);
        insertTimes(sb);
        selectTimes(sb);
        createDateTable(sb);
        insertDates(sb);
        selectDates(sb);
        createDateTimeTable(sb);
        insertDateTimes(sb);
        selectDateTimes(sb);
        sb.append("...\n");
        if (args.length == 0) {
            System.out.println(sb);
        } else {
            Writer out = new FileWriter(args[0]);
            try {
                out.write(sb.toString());
                out.close();
                System.out.println("Wrote to file " + args[0]);
            } finally {
                out.close();
            }
        }
    }

    private static void createTimeTable(StringBuilder out) {
        out.append("---\n");
        out.append("- CreateTable: times (ind int, t1 time, t2 time, t3 time," +
                   " t4 time)\n");
    }

    /**
     * Insert time values and add interval tests.
     */
    private static void insertTimes(StringBuilder out) throws IOException {
        int index = 1;
        for (String timeString : times) {
            DateTime time = new DateTime("T" + timeString, UTC);
            for (String intervalString : timeIntervals) {
                Period period =
                    timeIntervalFormatter.parsePeriod(intervalString);
                /*
                 * Insert row values: ind (index), t1 (time), t2 (time to
                 * subtract timeZero from, for interval), t3 (t1 + (t2 -
                 * timeZero)), t4 (t1 - (t2 - timeZero))
                 */
                out.append("---\n");
                out.append("- Statement: INSERT INTO times\n");
                out.append("    VALUES (");
                out.append(index).append(", '");
                timeFormatter.printTo(out, time);
                out.append("', '");
                timeFormatter.printTo(out, timeZero.plus(period));
                out.append("', '");
                timeFormatter.printTo(out, time.plus(period));
                out.append("', '");
                timeFormatter.printTo(out, time.minus(period));
                out.append("')\n");
                /* Test interval */
                for (int i = 0; i < 2; i++) {
                    boolean negate = (i == 1);
                    out.append("---\n");
                    out.append("- Statement: SELECT t1 + ");
                    insertTimeInterval(out, period, negate);
                    out.append(" FROM times WHERE ind = ").append(index);
                    out.append("\n");
                    out.append("- output: !select-engine { sys-mysql: [[null]], newtypes: [[null]], all: [['");
                    timeFormatter.printTo(
                        out, negate ? time.minus(period) : time.plus(period));
                    out.append("']] }\n");
                }
                index++;
            }
        }
    }

    private static void insertTimeInterval(StringBuilder out, Period period,
                                           boolean negate)
            throws IOException
    {
        out.append("INTERVAL '");
        if (negate) {
            out.append("-");
        }
        if (period.getDays() != 0) {
            if (period.getSeconds() != 0) {
                /* DD HH:MM:SS */
                new Formatter(out)
                    .format("%d %02d:%02d:%02d' DAY TO SECOND",
                            period.getDays(),
                            period.getHours(),
                            period.getMinutes(),
                            period.getSeconds());
            } else if (period.getMinutes() != 0) {
                /* DD HH:MM */
                new Formatter(out)
                    .format("%d %02d:%02d' DAY TO MINUTE",
                            period.getDays(),
                            period.getHours(),
                            period.getMinutes());
            } else if (period.getHours() != 0) {
                /* DD HH */
                new Formatter(out)
                    .format("%d %02d' DAY TO HOUR",
                            period.getDays(),
                            period.getHours());
            } else {
                /* DD */
                new Formatter(out).format("%d' DAY", period.getDays());
            }
        } else if (period.getHours() != 0) {
            if (period.getSeconds() != 0) {
                /* HH:MM:SS */
                new Formatter(out)
                    .format("%02d:%02d:%02d' HOUR TO SECOND",
                            period.getHours(),
                            period.getMinutes(),
                            period.getSeconds());
            } else if (period.getMinutes() != 0) {
                /* HH:MM */
                new Formatter(out)
                    .format("%02d:%02d' HOUR to MINUTE",
                            period.getHours(),
                            period.getMinutes());
            } else {
                /* HH */
                new Formatter(out).format("%d' HOUR", period.getHours());
            }
        } else if (period.getMinutes() != 0) {
            if (period.getSeconds() != 0) {
                /* MM:SS */
                new Formatter(out)
                    .format("%02d:%02d' MINUTE TO SECOND",
                            period.getMinutes(),
                            period.getSeconds());
            } else {
                /* MM */
                new Formatter(out).format("%d' MINUTE", period.getMinutes());
            }
        } else {
            /* SS */
            new Formatter(out).format("%d' SECOND", period.getSeconds());
        }
    }

    private static void selectTimes(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, t1 + (t2 - TIME('" +
                   timeZeroString + "')) FROM times\n");
        out.append("    WHERE t1 + (t2 - TIME('" + timeZeroString +
                   "')) <> t3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, t1 - (t2 - TIME('" +
                   timeZeroString + "')) FROM times\n");
        out.append("    WHERE t1 - (t2 - TIME('" + timeZeroString +
                   "')) <> t4\n");
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
                new DateTime(years[0] + "-" + months[0] + "-" + day, UTC),
                index);
        }
        for (String month : months) {
            index = insertDates(
                out,
                new DateTime(years[0] + "-" + month + "-" + days[0], UTC),
                index);
        }
        for (String year : years) {
            index = insertDates(
                out,
                new DateTime(year + "-" + months[0] + "-" + days[0], UTC),
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
                          Period.days(Integer.parseInt(interval)),
                          index++);
        }
        for (String interval : monthIntervals) {
            insertOneDate(out, date,
                          Period.months(Integer.parseInt(interval)),
                          index++);
            }
        for (String interval : yearIntervals) {
            Period period = Period.years(Integer.parseInt(interval));
            insertOneDate(out, date, period, index++);
            String monthInterval =
                monthIntervals[(index + 7) % monthIntervals.length];
            insertOneDate(out, date,
                          period.withMonths(Integer.parseInt(monthInterval)),
                          index++);
        }
        return index;
    }

    private static void insertOneDate(StringBuilder out, DateTime date,
                                      Period period, int index)
            throws IOException
    {
        Duration duration = period.toDurationFrom(dateZero);
        /*
         * Insert row values: ind (index), d1 (date), d2 (date to subtract
         * dateZero from, for interval), d3 (d1 + (d2 - dateZero)), d4 (d1 -
         * (d2 - dateZero))
         */
        out.append("---\n");
        out.append("- Statement: INSERT INTO dates\n");
        out.append("    VALUES (");
        out.append(index).append(", '");
        dateFormatter.printTo(out, date);
        out.append("', '");
        dateFormatter.printTo(out, dateZero.plus(duration));
        out.append("', '");
        dateFormatter.printTo(out, date.plus(duration));
        out.append("', '");
        dateFormatter.printTo(out, date.minus(duration));
        out.append("')\n");
        /* Test interval */
        for (int i = 0; i < 2; i++) {
            boolean negate = (i == 1);
            out.append("---\n");
            out.append("- Statement: SELECT d1 + ");
            insertDateInterval(out, period, negate);
            out.append(" FROM dates WHERE ind = ").append(index);
            out.append("\n");
            out.append("- output: [['");
            dateFormatter.printTo(
                out, negate ? date.minus(period) : date.plus(period));
            out.append("']]\n");
            /*
             * Disable negative values for YEAR TO MONTH intervals because of
             * Bug 913813: Adding YEAR TO MONTH INTERVAL with negative year
             * fails
             */
            if (period.getYears() != 0 && period.getMonths() != 0) {
                break;
            }
        }
    }

    private static void insertDateInterval(StringBuilder out, Period period,
                                           boolean negate)
            throws IOException
    {
        assert period.getHours() == 0 : period;
        assert period.getMinutes() == 0 : period;
        assert period.getSeconds() == 0 : period;
        out.append("INTERVAL '");
        if (negate) {
            out.append("-");
        }
        if (period.getDays() != 0) {
            int days = (int) period.toDurationFrom(dateZero).getStandardDays();
            /* DD */
            out.append(days).append("' DAY");
        } else if (period.getYears() != 0) {
            if (period.getMonths() != 0) {
                /* YYYY-MM */
                out.append(period.getYears()).append("-");
                out.append(period.getMonths());
                out.append("' YEAR TO MONTH");
            } else {
                /* YYYY */
                out.append(period.getYears()).append("' YEAR");
            }
        } else {
            /* MM */
            out.append(period.getMonths()).append("' MONTH");
        }
    }

    private static void selectDates(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, d1 + (d2 - DATE('" +
                   dateZeroString + "')) FROM dates\n");
        out.append("    WHERE d1 + (d2 - DATE('" + dateZeroString +
                   "')) <> d3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, d1 - (d2 - DATE('" +
                   dateZeroString + "')) FROM dates\n");
        out.append("    WHERE d1 - (d2 - DATE('" + dateZeroString +
                   "')) <> d4\n");
        out.append("- row_count: 0\n");
    }

    private static void createDateTimeTable(StringBuilder out) {
        out.append("---\n");
        out.append("- CreateTable: datetimes (ind int, dt1 datetime," +
                   " dt2 datetime,\n" +
                   "    dt3 datetime, dt4 datetime)\n");
    }

    private static void insertDateTimes(StringBuilder out)
            throws IOException
    {
        int index = 1;
        for (String day : days) {
            String timeString = times[index % times.length];
            String timeIntervalString =
                timeIntervals[index % timeIntervals.length];
            index = insertDateTimes(
                out,
                new DateTime(years[0] + "-" + months[0] + "-" + day +
                             "T" + timeString,
                             UTC),
                timeIntervalString,
                index);
        }
        for (String month : months) {
            String timeString = times[(index + 1) % times.length];
            index = insertDateTimes(
                out,
                new DateTime(years[0] + "-" + month + "-" + days[0] +
                             "T" + timeString,
                             UTC),
                "00:00:00",
                index);
        }
        for (String year : years) {
            String timeString = times[(index + 2) % times.length];
            index = insertDateTimes(
                out,
                new DateTime(year + "-" + months[0] + "-" + days[0] +
                             "T" + timeString,
                             UTC),
                "00:00:00",
                index);
        }
    }

    private static int insertDateTimes(StringBuilder out,
                                       DateTime date,
                                       String timeIntervalString,
                                       int index)
            throws IOException
    {
        Period timePeriod =
            timeIntervalFormatter.parsePeriod(timeIntervalString);
        for (String interval : dayIntervals) {
            insertOneDateTime(out, date,
                              timePeriod.plusDays(Integer.parseInt(interval)),
                              index++);
        }
        if (date.getHourOfDay() == 0 &&
            date.getMinuteOfHour() == 0 &&
            date.getSecondOfMinute() == 0)
        {
            for (String interval : monthIntervals) {
                insertOneDateTime(
                    out, date,
                    timePeriod.plusMonths(Integer.parseInt(interval)),
                    index++);
            }
            for (String interval : yearIntervals) {
                Period yearPeriod =
                    timePeriod.plusYears(Integer.parseInt(interval));
                insertOneDateTime(out, date, yearPeriod, index++);
                String montInterval =
                    monthIntervals[(index + 3) % monthIntervals.length];
                insertOneDateTime(
                    out, date,
                    yearPeriod.plusMonths(Integer.parseInt(interval)),
                    index++);
            }
        }
        return index;
    }

    private static void insertOneDateTime(StringBuilder out, DateTime date,
                                          Period period, int index)
            throws IOException
    {
        /*
         * Insert row values: ind (index), dt1 (datetime), dt2 (datetime to
         * subtract dateZero from, for interval), dt3 (dt1 + (dt2 - dateZero)),
         * dt4 (dt1 - (dt2 - dateZero))
         */
        out.append("---\n");
        out.append("- Statement: INSERT INTO datetimes\n");
        out.append("    VALUES (");
        out.append(index).append(", '");
        dateTimeFormatter.printTo(out, date);
        out.append("', '");
        dateTimeFormatter.printTo(out, dateZero.plus(period));
        out.append("',\n   '");
        dateTimeFormatter.printTo(out, date.plus(period));
        out.append("', '");
        dateTimeFormatter.printTo(out, date.minus(period));
        out.append("')\n");
        /* Test datetime interval */
        for (int i = 0; i < 2; i++) {
            boolean negate = (i == 1);
            out.append("---\n");
            out.append("- Statement: SELECT dt1 + ");
            insertDateTimeInterval(out, period, negate);
            out.append(" FROM datetimes WHERE ind = ").append(index);
            out.append("\n");
            out.append("- output: [[!re '");
            dateTimeFormatter.printTo(
                out, negate ? date.minus(period) : date.plus(period));
            out.append("([.]0)?']]\n");
            /*
             * Disable negative values for YEAR TO MONTH intervals because of
             * Bug 913813: Adding YEAR TO MONTH INTERVAL with negative year
             * fails
             */
            if (period.getYears() != 0 && period.getMonths() != 0) {
                break;
            }
        }
        index++;
        /*
         * TODO: Need to test timestamp intervals once we clear up the issue of
         * controlling the timezone for timestamps from tests.
         * -tblackman (01/05/2012)
         */
    }

    private static void insertDateTimeInterval(StringBuilder out, Period period,
                                               boolean negate)
            throws IOException
    {
        if (period.getHours() != 0 ||
            period.getMinutes() != 0 ||
            period.getSeconds() != 0)
        {
            insertTimeInterval(out, period, negate);
        } else {
            insertDateInterval(out, period, negate);
        }
    }

    private static void selectDateTimes(StringBuilder out) {
        out.append("---\n");
        out.append("- Statement: SELECT ind, dt1 + (dt2 - DATETIME('" +
                   dateZeroString + "')) FROM datetimes\n");
        out.append("    WHERE dt1 + (dt2 - DATETIME('" + dateZeroString +
                   "')) <> dt3\n");
        out.append("- row_count: 0\n");
        out.append("---\n");
        out.append("- Statement: SELECT ind, dt1 - (dt2 - DATETIME('" +
                   dateZeroString + "')) FROM datetimes\n");
        out.append("    WHERE dt1 - (dt2 - DATETIME('" + dateZeroString +
                   "')) <> dt4\n");
        out.append("- row_count: 0\n");
    }
}
