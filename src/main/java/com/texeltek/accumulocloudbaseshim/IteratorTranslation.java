package com.texeltek.accumulocloudbaseshim;


import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;

import java.io.IOException;
import java.util.Map;

public final class IteratorTranslation {
    private IteratorTranslation() {
    }

    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    public static IteratorSetting translate(IteratorSetting cfg) {
        try {
            if (cfg.getIteratorClass().equals(RegExFilter.class.getName())) {
                return translateRegExFilter(cfg);
            } else if (isCloudbaseFilter(cfg.getIteratorClass())) {
                return translateCloudbaseFilter(cfg);
            } else {
                return translateCloudbaseIterator(cfg);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static IteratorSetting translateRegExFilter(IteratorSetting cfg) throws IOException {
        IteratorSetting result = new IteratorSetting(cfg.getPriority(), cfg.getName(),
                cloudbase.core.iterators.RegExIterator.class.getName());
        transferOptions(cfg, result);
        return result;
    }

    private static boolean isCloudbaseFilter(String iteratorClass) {
        try {
            Class<?> clazz = Class.forName(convertToCloudbaseFilterPackage(iteratorClass));
            return cloudbase.core.iterators.filter.Filter.class.isAssignableFrom(clazz);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static IteratorSetting translateCloudbaseFilter(IteratorSetting cfg) throws IOException {
        IteratorSetting result = new IteratorSetting(cfg.getPriority(), cfg.getName(),
                cloudbase.core.iterators.FilteringIterator.class.getName());
        result.addOption("0", convertToCloudbaseFilterPackage(cfg.getIteratorClass()));
        for (Map.Entry<String, String> option : cfg.getOptions().entrySet()) {
            result.addOption("0." + option.getKey(), option.getValue());
        }
        return result;
    }

    private static IteratorSetting translateCloudbaseIterator(IteratorSetting cfg) throws IOException {
        IteratorSetting result = translateIteratorPackage(cfg, "org.apache.accumulo.core.iterators.user", "cloudbase.core.iterators");
        return translateIteratorPackage(result, "org.apache.accumulo.core.iterators", "cloudbase.core.iterators");
    }

    private static String convertToCloudbaseFilterPackage(String className) {
        return className.replace("org.apache.accumulo.core.iterators.user", "cloudbase.core.iterators.filter");
    }

    private static IteratorSetting translateIteratorPackage(IteratorSetting cfg, String prefix, String replacement) {
        if (cfg.getIteratorClass().startsWith(prefix)) {
            IteratorSetting result = new IteratorSetting(cfg.getPriority(), cfg.getName(),
                    replacement + cfg.getIteratorClass().substring(prefix.length()));
            transferOptions(cfg, result);
            return result;
        }

        return cfg;
    }

    private static void transferOptions(IteratorSetting from, IteratorSetting to) {
        for (Map.Entry<String, String> option : from.getOptions().entrySet()) {
            to.addOption(option.getKey(), option.getValue());
        }
    }
}
