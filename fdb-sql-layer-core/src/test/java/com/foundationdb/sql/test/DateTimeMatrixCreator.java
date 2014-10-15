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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/*
 * This class creates the database definition and outputs a data set for 
 * use in the functional testing system.  It produces a YAML 
 * for inclusion in testing with other yaml files.  A SQL file is created to 
 * allow for direct mySQL comparisons.
 * */
public class DateTimeMatrixCreator implements Runnable {

    private static final int MAX_TIME = 838;
    public static int counter = 1;
    public static int min_counter = 1;
    public static int sec_counter = 50;
    public static int hour_counter = 3;
    StringBuffer data = new StringBuffer();
    StringBuffer sql_data = new StringBuffer();

    /**
     * @param args
     */
    public static void main(String[] args) {
        DateTimeMatrixCreator m = new DateTimeMatrixCreator();
        m.run();
    }
    
    private String genSQL(String year, String month, String day, String hour,
                          String minute, String second, String millisecond, String timestamp,
                          String day_of_week, String weekday, String yearweek,
                          String dayname, String dayOfmonth, String weekofyear,
                          boolean sql)
    {
         String database = "datetime_matrix";
         if (sql) {
            database = "test." + database;
        }

        return "insert into "
                + database
                + " (id, date_field, time_field, timestamp_field, "
                + "expected_year, expected_month, expected_day, expected_hour, "
                + "expected_minute, expected_second, expected_millisecond,day_of_week, weekday, weekofyear, yearweek, dayname, dayofmonth) "
                + "values (" + (counter++) + ",'" + year + "-" + month + "-"
                + day + "', '" + hour + ":" + minute + ":" + second + "',"
                + timestamp + " , " + year + ", " + month + " , " + day + " , "
                + hour + " , " + minute + " , " + second + " , " + millisecond
                + " , " + day_of_week + " , " + weekday + " , " + weekofyear
                + " , " + yearweek + ", '" + dayname + "'," + dayOfmonth + ")";
    }

    private String genSQL(Calendar data, boolean sql) {
        String year = String.format("%04d", data.get(Calendar.YEAR));
        String month = String.format("%02d", data.get(Calendar.MONTH) + 1);
        String day = String.format("%02d", data.get(Calendar.DAY_OF_MONTH));
        String hour = String.format("%02d", data.get(Calendar.HOUR_OF_DAY));
        String minute = String.format("%02d", data.get(Calendar.MINUTE));
        String second = String.format("%02d", data.get(Calendar.SECOND));
        String millisecond = String.format("%03d",
                data.get(Calendar.MILLISECOND));
        String day_of_week = String.valueOf(data.get(Calendar.DAY_OF_WEEK));
        String weekday = String.valueOf((data.get(Calendar.DAY_OF_WEEK) + 5) % 7);
        String weekofyear = String.valueOf(data.get(Calendar.WEEK_OF_YEAR));
        String yearweek = year
                + String.valueOf(data.get(Calendar.WEEK_OF_YEAR));
        String dayname = convertDayName(data.get(Calendar.DAY_OF_WEEK));
        String dayOfmonth = String.valueOf(data.get(Calendar.DAY_OF_MONTH));
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
                + "expected_minute, expected_second, expected_millisecond,day_of_week, weekday, weekofyear, yearweek, dayname, dayofmonth) "
                + "values (" + (counter++) + ",'" + year + "-" + month + "-"
                + day + "', '" + hour + ":" + minute + ":" + second + "',"
                + timestamp + " , " + year + ", " + month + " , " + day + " , "
                + hour + " , " + minute + " , " + second + " , " + millisecond
                + " , " + day_of_week + " , " + weekday + " , " + weekofyear
                + " , " + yearweek + ", '" + dayname + "'," + dayOfmonth + ")";
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

    @Override
    public void run() {
        data.append("# generated by com.foundationdb.sql.test.DateTimeMatrixCreator on "
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
                + "      dayname varchar(15), week int, dayofmonth int"
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
        //processYear(1950);
        //processYear(1473); java.util.Calendar does not handle correctly years before 1900
        wackyYearTest();
        data.append("..." + System.getProperty("line.separator"));
        try {
            
            String path = System.getProperty("user.dir")
                    + "/src/test/resources/com/foundationdb/sql/pg/yaml/functional/";
            save(path + "all-datetime-schema.yaml", data);
            //save("data.sql", sql_data);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * java.util.Calendar doesn't handle wacky years very well.
     * So we'll just hard-code the expecteds for those cases. (as per MySQL)
     * 
     */
    private void wackyYearTest() {
        // year 300 
        this.recordSQL("0300", "01", "01", "23", "59", "59", "0", "null", "2", "0", "29953", "Monday", "01", "01");
               
        // year 0
        this.recordSQL("0000", "03", "01", "01", "30", "25", "0", "null", "4", "2", "9", "Wednesday", "01", "9");
        
        // year 1
        this.recordSQL("0001", "05", "29", "12", "01", "01", "0", "null", "3", "1", "121", "Tuesday", "29", "22");
               
        // year 2500
        this.recordSQL("2500", "12", "31", "12", "59", "59", "0", "null", "6", "4", "250052", "Friday", "31", "52");
        
        // year 9999
        this.recordSQL("9999", "11", "07", "12", "30", "10", "0", "null", "1", "6", "999945", "Sunday", "07", "44");
       
    }

    private void processYear(int year) {
        Calendar cal = Calendar.getInstance();
        cal.setLenient(false);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.YEAR, year);
        for (int month = 0; month <= cal.getActualMaximum(Calendar.MONTH); month++) {
            cal.set(Calendar.MONTH, month);
            for (int day = 1; day < cal.getActualMaximum(Calendar.DAY_OF_MONTH); day++) {
                try {
                    cal.set(Calendar.DAY_OF_MONTH, day);
                    if (min_counter < 60) {
                        min_counter++;
                    } else {
                        min_counter = 0;
                    }
                    if (sec_counter < 60) {
                        sec_counter++;
                    } else {
                        sec_counter = 0;
                    }
                    if (hour_counter < 24) {
                        hour_counter++;
                    } else {
                        hour_counter = 0;
                    }
                    if (hour_counter <= 24 && hour_counter >= 0) {
                        cal.set(Calendar.HOUR, hour_counter);
                    } else {
                        cal.set(Calendar.HOUR, 1);
                    }
                    if (min_counter <= 60 && min_counter > 0) {
                        cal.set(Calendar.MINUTE, min_counter);
                    } else {
                        cal.set(Calendar.MINUTE, 7);
                    }
                    if (sec_counter < 60 && sec_counter > 0) {
                        cal.set(Calendar.SECOND, sec_counter);
                    } else {
                        cal.set(Calendar.SECOND, 15);
                    }
                    recordSQL(cal);
                } catch (Exception e) {
                    /// just skip bad dates
                    System.out.println("bad date");
                    cal = Calendar.getInstance();
                    cal.setLenient(false);
                    cal.set(Calendar.MILLISECOND, 0);
                    cal.set(Calendar.YEAR, year);
                    cal.set(Calendar.MONTH, month);
                    cal.set(Calendar.DAY_OF_MONTH, 1);
                }

            }
        }
    }

    private void recordSQL(Calendar cal) {
        data.append("---\n" + "- Statement: " + genSQL(cal, false)
                + System.getProperty("line.separator"));
        sql_data.append(genSQL(cal, true) + ";"
                + System.getProperty("line.separator"));
    }

    private void recordSQL(String year, String month, String day, String hour,
                          String minute, String second, String millisecond, String timestamp,
                          String day_of_week, String weekday, String yearweek,
                          String dayname, String dayOfmonth, String weekofyear)
    {        
        data.append("---\n" + "- Statement: " + genSQL(year, month, day, hour,
                                                       minute, second, millisecond, timestamp,
                                                       day_of_week, weekday, yearweek,
                                                       dayname, dayOfmonth, weekofyear,
                                                       false)
                + System.getProperty("line.separator"));
        sql_data.append(genSQL(year, month, day, hour,
                               minute, second, millisecond, timestamp,
                               day_of_week, weekday, yearweek,
                               dayname, dayOfmonth, weekofyear,
                               true) 
                + ";"
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
