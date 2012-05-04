package com.akiban.collation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ibm.icu.text.Collator;

public class CStringTest {

    @Test
    public void testCompareTo() throws Exception {
        final Collator collator =  CollatorFactory.getCollator("en_US");
        final CString cs1 = new CString("RedFox", collator);
        final CString cs2 = new CString("REDFOX", collator);
        final CString cs3 = new CString("greenfox", collator);
        final CString cs4 = new CString("YELLOWFOX", collator);
        
        assertEquals("Should be equal", 0, cs1.compareTo(cs2));
        assertTrue("Should be Less", cs1.compareTo(cs3) > 0);
        assertTrue("Should be Greater", cs1.compareTo(cs4) < 0);
        
        assertFalse("Should not have constructed byte array", cs1.hasSortKeyBytes());
        assertFalse("Should not have constructed byte array", cs2.hasSortKeyBytes());
        final CString cs1a = new CString(collator);
        cs1a.putSortKeyBytes(cs1.getSortKeyBytes());
        final CString cs2a = new CString(collator);
        cs2a.putSortKeyBytes(cs2.getSortKeyBytes());
        assertEquals("Should be equal", 0, cs1a.compareTo(cs2a));
        assertEquals("Should be equal", 0, cs1.compareTo(cs2a));
        
        String s;
        s = cs1.toString();
        assertEquals("Should be equal", "RedFox", s);
        s = cs1a.toString();
        assertTrue("Should be hex-coded", s.toString().startsWith("CString["));
    }
}
