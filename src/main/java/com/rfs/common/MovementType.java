package com.rfs.common;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public enum MovementType {
    EVERYTHING, METADATA, DATA;

    public static class ArgsConverter implements IStringConverter<MovementType> {
        @Override
        public MovementType convert(String value) {
            switch (value) {
                case "everything":
                    return MovementType.EVERYTHING;
                case "metadata":
                    return MovementType.METADATA;
                case "data":
                    return MovementType.DATA;
                default:
                    throw new ParameterException("Invalid source version: " + value);
            }
        }
    }
}
