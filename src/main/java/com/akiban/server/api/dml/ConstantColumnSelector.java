
package com.akiban.server.api.dml;

public final class ConstantColumnSelector implements ColumnSelector {

    public static final ColumnSelector ALL_ON = new ConstantColumnSelector(true);
    public static final ColumnSelector ALL_OFF = new ConstantColumnSelector(false);

    private final boolean value;

    private ConstantColumnSelector(boolean value) {
        this.value = value;
    }

    @Override
    public boolean includesColumn(int columnPosition) {
        return value;
    }
}
