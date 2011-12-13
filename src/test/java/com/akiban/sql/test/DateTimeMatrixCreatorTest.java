package com.akiban.sql.test;

import org.junit.Assert;
import org.junit.Test;

public class DateTimeMatrixCreatorTest {

    @Test
    public void test() {
        DateTimeMatrixCreator c = new DateTimeMatrixCreator();
        Assert.assertEquals("6",c.convertWeekday("2", 3, false));
        Assert.assertEquals("7",c.convertWeekday("3", 3, false));
        Assert.assertEquals("1",c.convertWeekday("4", 3, false));
        Assert.assertEquals("2",c.convertWeekday("5", 3, false));
        Assert.assertEquals("3",c.convertWeekday("6", 3, false));
        Assert.assertEquals("4",c.convertWeekday("7", 3, false));
        Assert.assertEquals("5",c.convertWeekday("1", 3, false));
    }

    @Test
    public void test2() {
        DateTimeMatrixCreator c = new DateTimeMatrixCreator();
        Assert.assertEquals("4",c.convertWeekday("2", -2, true));
        Assert.assertEquals("5",c.convertWeekday("3", -2, true));
        Assert.assertEquals("6",c.convertWeekday("4", -2, true));
        Assert.assertEquals("0",c.convertWeekday("5", -2, true));
        Assert.assertEquals("1",c.convertWeekday("6", -2, true));
        Assert.assertEquals("2",c.convertWeekday("7", -2, true));
        Assert.assertEquals("3",c.convertWeekday("1", -2, true));
        
    }

    
}
