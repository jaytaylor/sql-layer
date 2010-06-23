/**
 * 
 */
package com.akiban.cserver.store;
import java.io.IOException;
import java.nio.*;

/**
 * @author percent
 *
 */
public interface FieldArray {

    //public enum Type {FixedLength, VariableLength}
    public int getNextFieldSize() throws IOException;
    //public boolean rewind(int amount);
    public boolean copyNextField(byte[] byteArray, int offset) throws IOException;
    public long getColumnSize();
    //public Type getType();
}
