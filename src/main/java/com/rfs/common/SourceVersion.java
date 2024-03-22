package com.rfs.common;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public enum SourceVersion {
    ES_6_8, ES_7_10;

    public static class ArgsConverter implements IStringConverter<SourceVersion> {
        @Override
        public SourceVersion convert(String value) {
            switch (value) {
                case "es_6_8":
                    return SourceVersion.ES_6_8;
                case "es_7_10":
                    return SourceVersion.ES_7_10;
                default:
                    throw new ParameterException("Invalid source version: " + value);
            }
        }
    }
}
