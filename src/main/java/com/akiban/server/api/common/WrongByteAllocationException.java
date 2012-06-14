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

package com.akiban.server.api.common;

public final class WrongByteAllocationException extends RuntimeException {

    /**
     * Checks if the number of allocated bytes is the same as the number of
     * required bytes, and throws a WrongByteAllocationException if they're not.
     * @param allocated the number of bytes the invoker expected to allocate
     * @param required the number of bytes a method required
     * @throws WrongByteAllocationException if the two inputs aren't equal
     */
    public static void ifNotEqual(int allocated, int required) {
        if (allocated != required) {
            throw new WrongByteAllocationException(allocated, required);
        }
    }

    /**
     * Complains that the number of bytes allocated (by a method's invoker) is
     * not the same as the number of bytes required (by the method).
     * @param allocatedBytes the number of bytes the invoker expected to allocate
     * @param requiredBytes the number of bytes a method required
     */
    private WrongByteAllocationException(int allocatedBytes, int requiredBytes) {
        super(String.format("Caller expected %s byte%s, but %s required",
                allocatedBytes, allocatedBytes == 1 ? "s" : "", requiredBytes));
    }
}
