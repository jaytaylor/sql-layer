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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ibm.icu.util.ULocale;

public class AkCollatorFactoryTest {

    private final static int NTHREADS = 10;
    
    @Test
    public void getAkCollator() throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator("sv_se_ci");
        assertEquals("sv_SE", collator.getCollator().getLocale(ULocale.VALID_LOCALE).getName());
    }
    
    @Test
    public void uniquePerThread() throws Exception {
        final AtomicInteger threadIndex = new AtomicInteger();
        final AkCollator[] array = new AkCollator[NTHREADS];
        Thread[] threads = new Thread[NTHREADS];
        for (int i = 0; i < NTHREADS;i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    int index = threadIndex.getAndIncrement();
                    array[index] = AkCollatorFactory.getAkCollator("sv_se_ci");
                }
            });
        }
        for (int i = 0; i < NTHREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NTHREADS; i++) {
            threads[i].join();
        }
        for (int i = 0; i < NTHREADS; i++) {
            assertNotNull("Null", array[i]);
            for (int j = 0; j < i; j++) {
                assertTrue("Not unique", array[i] != array[j]);
            }
        }
    }
    
    @Test
    public void makeMySQLCollator() throws Exception {
        final AkCollator collator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        assertEquals("Collector should have correct name", "latin1_swedish_ci", collator.getName());
    }
}
