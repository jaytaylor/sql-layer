
package com.akiban.server.store.statistics;

public class HistogramEntry extends HistogramEntryDescription {
    private byte[] keyBytes;

    HistogramEntry(String keyString, byte[] keyBytes,
                   long equalCount, long lessCount, long distinctCount) {
        super(keyString, equalCount, lessCount, distinctCount);
        this.keyBytes = keyBytes;
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }
}
