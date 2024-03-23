package com.rfs.common;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Transformer {
    public ObjectNode transformGlobalMetadata(ObjectNode root);
    public ObjectNode transformIndexMetadata(ObjectNode root);
    
}
