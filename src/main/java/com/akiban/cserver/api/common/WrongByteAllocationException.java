package com.akiban.cserver.api.common;

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
