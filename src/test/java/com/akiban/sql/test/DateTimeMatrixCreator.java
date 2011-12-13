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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class DateTimeMatrixCreator implements Runnable {

    private static final int MAX_TIME = 838;
    public static int counter = 1;
    public static int min_counter = 1;
    public static int sec_counter = 50;
    StringBuffer data = new StringBuffer();
    StringBuffer sql_data = new StringBuffer();

    /**
     * @param args
     */
    public static void main(String[] args) {
        DateTimeMatrixCreator m = new DateTimeMatrixCreator();
        m.run();
    }

    private String genSQL(Calendar data, boolean sql) {
        String year = String.format("%04d", data.get(Calendar.YEAR));
        String month = String.format("%02d", data.get(Calendar.MONTH));
        String day = String.format("%02d", data.get(Calendar.DAY_OF_MONTH));
        String hour = String.format("%02d", data.get(Calendar.HOUR_OF_DAY));
        String minute = String.format("%02d", data.get(Calendar.MINUTE));
        String second = String.format("%02d", data.get(Calendar.SECOND));
        String millisecond = String.format("%03d",
                data.get(Calendar.MILLISECOND));
        String day_of_week = convertWeekday(
                String.valueOf(data.get(Calendar.DAY_OF_WEEK)), 3, false);

        String weekday = convertWeekday(
                String.valueOf(data.get(Calendar.DAY_OF_WEEK)), -2, true);
        String weekofyear = String.valueOf(data.get(Calendar.WEEK_OF_YEAR));
        String yearweek = year
                + String.valueOf(data.get(Calendar.WEEK_OF_YEAR));
        String dayname = convertDayName(data.get(Calendar.DAY_OF_WEEK));
        String database = "datetime_matrix";
        if (sql) {
            database = "test." + database;
        }
        //The TIMESTAMP data type is used for values that contain both date and time parts. 
        //TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC. 
        String timestamp = "null";
        if ((data.get(Calendar.YEAR) > 1969)
                && (data.get(Calendar.YEAR) < 2039)) {
            timestamp = "'" + year + "-" + month + "-" + day + " " + hour + ":"
                    + minute + ":" + second + "'";
        }

        return "insert into "
                + database
                + " (id, date_field, time_field, timestamp_field, "
                + "expected_year, expected_month, expected_day, expected_hour, "
                + "expected_minute, expected_second, expected_millisecond,day_of_week, weekday, weekofyear, yearweek, dayname) "
                + "values (" + (counter++) + ",'" + year + "-" + month + "-"
                + day + "', '" + hour + ":" + minute + ":" + second + "',"
                + timestamp + " , " + year + ", " + month + " , " + day + " , "
                + hour + " , " + minute + " , " + second + " , " + millisecond
                + " , " + day_of_week + " , " + weekday + " , " + weekofyear
                + " , " + yearweek + ", '" + dayname + "')";
    }

    private String convertDayName(int day_of_week) {
        String retVal = null;
        switch (day_of_week) {
            case Calendar.SUNDAY:
                retVal = "Sunday";
                break;
            case Calendar.MONDAY:
                retVal = "Monday";
                break;
            case Calendar.TUESDAY:
                retVal = "Tuesday";
                break;
            case Calendar.WEDNESDAY:
                retVal = "Wednesday";
                break;
            case Calendar.THURSDAY:
                retVal = "Thursday";
                break;
            case Calendar.FRIDAY:
                retVal = "Friday";
                break;
            case Calendar.SATURDAY:
                retVal = "Saturday";
                break;
        }
        return retVal;
    }

    /*
     * 
     * */

    // takes in (1 = Sunday, 2 = Monday, …, 7 = Saturday)
    // result is (0 = Monday, 1 = Tuesday, … 5=sat, 6 = Sunday).
    public String convertWeekday(String valueOf, int indexOff, boolean zeroBase) {
        int value = Integer.parseInt(valueOf);
        int returnValue = value - indexOff;
        if (returnValue < getMin(zeroBase)) {
            returnValue = returnValue + getMax(zeroBase);
        }
        if (returnValue > getMax(zeroBase)) {
            returnValue = returnValue - 7;
        }
        return String.valueOf(returnValue);
    }

    private int getMax(boolean zeroBase) {
        return zeroBase ? 6 : 7;
    }

    private int getMin(boolean zeroBase) {
        return zeroBase ? 0 : 1;
    }

    @Override
    public void run() {
        data.append("# generated by com.akiban.sql.test.DateTimeMatrixCreator on "
                + new Date() + System.getProperty("line.separator"));
        data.append("# Create a table with all supported data types"
                + System.getProperty("line.separator") + "---"
                + System.getProperty("line.separator")
                + "- CreateTable: datetime_matrix ("
                + System.getProperty("line.separator") + "      id integer,"
                + System.getProperty("line.separator")
                + "      date_field date,"
                + System.getProperty("line.separator")
                + "      time_field time,"
                + System.getProperty("line.separator")
                + "      timestamp_field timestamp,"
                + System.getProperty("line.separator")
                + "      expected_year integer,"
                + System.getProperty("line.separator")
                + "      expected_month integer,"
                + System.getProperty("line.separator")
                + "      expected_day integer,"
                + System.getProperty("line.separator")
                + "      expected_hour integer,"
                + System.getProperty("line.separator")
                + "      expected_minute integer,"
                + System.getProperty("line.separator")
                + "      expected_second integer,"
                + System.getProperty("line.separator")
                + "      expected_millisecond integer,"
                + System.getProperty("line.separator")
                + "      day_of_week integer,"
                + System.getProperty("line.separator")
                + "      weekday integer,"
                + System.getProperty("line.separator")
                + "      weekofyear integer,"
                + System.getProperty("line.separator")
                + "      yearweek integer,"
                + System.getProperty("line.separator")
                + "      dayname varchar(15)"
                + System.getProperty("line.separator") + "      )"

                + System.getProperty("line.separator"));
        sql_data.append("use test;" + System.getProperty("line.separator"));
        sql_data.append("truncate table test.datetime_matrix;"
                + System.getProperty("line.separator"));
        processYear(1999);
        processYear(2000);
        processYear(2012);
        processYear(2011);
        processYear(2500);
        processYear(1950);
        processYear(1473);
        wackyYearTest();
        data.append("..." + System.getProperty("line.separator"));
        try {
            //TODO find better spot for output
            save("all-datetime-schema.yaml", data);
            save("data.sql", sql_data);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private void wackyYearTest() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.YEAR, 300);
        recordSQL(cal);
        cal.set(Calendar.YEAR, 0);
        recordSQL(cal);
        cal.set(Calendar.YEAR, -200);
        recordSQL(cal);
        cal.set(Calendar.YEAR, -2200);
        recordSQL(cal);
        cal.set(Calendar.YEAR, 2500);
        recordSQL(cal);
        cal.set(Calendar.YEAR, 9999);
        recordSQL(cal);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        recordSQL(cal);
        cal.set(Calendar.HOUR, 11);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        recordSQL(cal);
        cal.set(Calendar.HOUR, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        recordSQL(cal);
        cal.set(Calendar.YEAR, 1999);
        cal.set(Calendar.MONTH, 11);
        cal.set(Calendar.DAY_OF_MONTH, 31);
        recordSQL(cal);
    }

    private void processYear(int year) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.YEAR, year);
        for (int month = 1; month < cal.getActualMaximum(Calendar.MONTH); month++) {
            cal.set(Calendar.MONTH, month);
            for (int day = 1; day < cal.getActualMaximum(Calendar.DAY_OF_MONTH); day++) {
                cal.set(Calendar.DAY_OF_MONTH, day);
                if (min_counter < 999) {
                    min_counter++;
                } else {
                    min_counter = -999;
                }
                if (sec_counter < 999) {
                    sec_counter++;
                } else {
                    sec_counter = -999;
                }
                if (min_counter < MAX_TIME && min_counter > -MAX_TIME) {
                    cal.set(Calendar.MINUTE, min_counter);
                } else {
                    cal.set(Calendar.MINUTE, MAX_TIME);
                }
                if (sec_counter < MAX_TIME && sec_counter > -MAX_TIME) {
                    cal.set(Calendar.SECOND, sec_counter);
                } else {
                    cal.set(Calendar.SECOND, MAX_TIME);
                }
                recordSQL(cal);
            }
        }
    }

    private void recordSQL(Calendar cal) {
        data.append("---\n" + "- Statement: " + genSQL(cal, false)
                + System.getProperty("line.separator"));
        sql_data.append(genSQL(cal, true) + ";"
                + System.getProperty("line.separator"));
    }

    private void save(String filename, StringBuffer data) throws IOException {
        try {
            // Create file
            FileWriter fstream = new FileWriter(filename);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(data.toString());
            // Close the output stream
            out.close();
        } catch (Exception e) {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
        File f = new File(filename);
        System.out.println(f.getCanonicalPath());
        System.out.println(data.toString());

    }

}
