package com.akiban.util;

public class TapMXBeanImpl implements TapMXBean {

    @Override
    public String getReport() {
        return Tap.report();
    }

    @Override
    public void reset(String regExPattern) {
        Tap.reset(regExPattern);
    }

    @Override
    public void resetAll() {
        Tap.reset(".*");
    }

    @Override
    public void disableAll() {
        Tap.setEnabled(".*", false);
    }

    @Override
    public void enableAll() {
        Tap.setEnabled(".*", true);
    }

    @Override
    public void setEnabled(final String regExPattern, final boolean on) {
        Tap.setEnabled(regExPattern, on);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setCustomTap(final String regExPattern, final String className)
            throws Exception {
        final Class<?> clazz = (Class.forName(className));
        if (Tap.class.isAssignableFrom(clazz)) {
            Class<? extends Tap> c = (Class<? extends Tap>) clazz;
            Tap.setCustomTap(regExPattern, c);
        } else {
            throw new ClassCastException(className + " is not a "
                    + Tap.class.getName());
        }
    }

    @Override
    public TapReport[] getReports(final String regExPattern) {
        return Tap.getReport(regExPattern);
    }
}
