package com.rfs.common;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public enum ClusterVersion {
    ES_6_8, ES_7_10, OS_2_11;

    public static class ArgsConverter implements IStringConverter<ClusterVersion> {
        @Override
        public ClusterVersion convert(String value) {
            switch (value) {
                case "es_6_8":
                    return ClusterVersion.ES_6_8;
                case "es_7_10":
                    return ClusterVersion.ES_7_10;
                case "os_2_11":
                    return ClusterVersion.OS_2_11;
                default:
                    throw new ParameterException("Invalid source version: " + value);
            }
        }
    }
}
