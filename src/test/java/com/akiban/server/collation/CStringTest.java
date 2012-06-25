/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.collation;

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
