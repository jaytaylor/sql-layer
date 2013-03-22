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

package com.akiban.server;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.persistit.Key;

public class PersistitKeyPValueTarget implements PValueTarget {

    private final static int DIGEST_SIZE = 16;

    private final static ThreadLocal<MessageDigest> md5Digest = new ThreadLocal<MessageDigest>() {
        public MessageDigest initialValue() {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                return md;
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("No MD5 MessageDigest algorithm available");
            }
        }
    };


    // object state

    private Key key;
    private int maximumKeySegmentLength = Integer.MAX_VALUE;
    
    
    public PersistitKeyPValueTarget() {
        // default constructor
    }
    
    public PersistitKeyPValueTarget(int max) {
        assert max > DIGEST_SIZE;
        maximumKeySegmentLength = max;
    }
    
    
    // PersistitKeyPValueTarget interface

    @Override
    public boolean supportsCachedObjects() {
        return true;
    }

    public void attach(Key key) {
        this.key = key;
    }
    
    // PValueTarget interface
    
    @Override
    public TInstance tInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        key.append(null);
    }

    @Override
    public void putBool(boolean value) {
        key.append(value);
    }

    @Override
    public void putInt8(byte value) {
        key.append((long)value);
    }

    @Override
    public void putInt16(short value) {
        key.append((long)value);
    }

    @Override
    public void putUInt16(char value) {
        key.append((long)value);
    }

    @Override
    public void putInt32(int value) {
        key.append((long)value);
    }

    @Override
    public void putInt64(long value) {
        key.append(value);
    }

    @Override
    public void putFloat(float value) {
        key.append(value);
    }

    @Override
    public void putDouble(double value) {
        key.append(value);
    }

    @Override
    public void putBytes(byte[] value) {
        key.append(value);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        final int size = key.getEncodedSize();
        if (collator == null) {
            key.append(value);
        } else {
            collator.append(key, value);
        }
        digest(size);
    }

    @Override
    public void putObject(Object object) {
        final int size = key.getEncodedSize();
        key.append(object);
        digest(size);
    }

    // object interface

    @Override
    public String toString() {
        return key().toString();
    }

    // for use by this class

    protected final Key key() {
        return key;
    }
    
    private void digest(final int initialSize) {
        final int appended = key.getEncodedSize() - initialSize;
        if (appended <= maximumKeySegmentLength) {
            return;
        }
        MessageDigest md = md5Digest.get();
        md.reset();
        md.update(key.getEncodedBytes(), initialSize, appended);
        final byte[] digest = md.digest();
        assert digest.length == DIGEST_SIZE;
        /*
         * No zeroes allowed in the digest. We simply lose a few bits of
         * fidelity here by replacing any 0 with 0xFF.
         */
        for (int i = 0; i < DIGEST_SIZE; i++) {
            if (digest[i] == 0) {
                digest[i] = (byte)0xFF;
            }
        }
        System.arraycopy(digest, 0, key.getEncodedBytes(),  initialSize + maximumKeySegmentLength - DIGEST_SIZE, DIGEST_SIZE);
        key.setEncodedSize(initialSize + maximumKeySegmentLength + 1);
        key.getEncodedBytes()[key.getEncodedSize() - 1] = 0;
    }
}
