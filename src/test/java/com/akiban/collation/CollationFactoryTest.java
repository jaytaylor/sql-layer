package com.akiban.collation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;


public class CollationFactoryTest {

    private final static int NTHREADS = 10;
    
    @Test
    public void getACollator() throws Exception {
        final Collator collator = CollatorFactory.getCollator("sw_SW");
        assertEquals("sw", collator.getLocale(ULocale.VALID_LOCALE).getName());
    }
    
    @Test
    public void uniquePerThread() throws Exception {
        final AtomicInteger threadIndex = new AtomicInteger();
        final Collator[] array = new Collator[NTHREADS];
        Thread[] threads = new Thread[NTHREADS];
        for (int i = 0; i < NTHREADS;i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    int index = threadIndex.getAndIncrement();
                    array[index] = CollatorFactory.getCollator("sw_SW");
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
}
