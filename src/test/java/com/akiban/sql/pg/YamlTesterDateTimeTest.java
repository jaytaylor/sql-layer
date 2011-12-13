package com.akiban.sql.pg;

import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import com.akiban.sql.pg.YamlTester.Regexp;
import com.akiban.sql.pg.YamlTester.*;


public class YamlTesterDateTimeTest {

    @Test
    public void testTimeTag() {
        Calendar cal = Calendar.getInstance();
        String time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        test(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.SECOND, -30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        test(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.SECOND, 30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        test(time);
    }
    
    @Test
    public void testTimeTag_Negative() {
        Calendar cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, 5);
        String time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        testFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        testFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, 2);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        testFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, -1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND));
        testFail(time);
    }
    
    @Test
    public void testDateTimeTag() {
        Calendar cal = Calendar.getInstance();
        String time = formatDateTime(cal);
        testdt(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.SECOND, -30);
        time = formatDateTime(cal);
        testdt(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.SECOND, 30);
        time = formatDateTime(cal);
        testdt(time);
    }

    private String formatDateTime(Calendar cal) {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%03d", cal.get(Calendar.YEAR),cal.get(Calendar.MONTH)+1,cal.get(Calendar.DAY_OF_MONTH),cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND),cal.get(Calendar.MILLISECOND));
    }
    
    @Test
    public void testDateTimeTag_Negative() {
        Calendar cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, 5);
        String time = formatDateTime(cal);
        testdtFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, 2);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = Calendar.getInstance();
        cal.roll(Calendar.MINUTE, -1);
        time = formatDateTime(cal);
        testdtFail(time);
    }
    
    private static void test(String output) {
        boolean result = new TimeChecker().compareOutput(output);
        if (!result) {
            fail("Time check failed with "+output);
        } 
    }
    
    private static void testFail(String output) {
        boolean result = new TimeChecker().compareOutput(output);
        if (result) {
            fail("Time check failed with "+output);
        } 
    }
    
    private static void testdt(String output) {
        boolean result = new DateTimeChecker().compareOutput(output);
        if (!result) {
            fail("Time check failed with "+output);
        } else {
            System.out.println(output);
        }
    }
    
    private static void testdtFail(String output) {
        boolean result = new DateTimeChecker().compareOutput(output);
        if (result) {
            fail("Time check failed with "+output);
        } else {
            System.out.println(output);
        }
    }
    
   
}
