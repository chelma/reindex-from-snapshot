package com.rfs.version_es_7_10;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.Transformer;

public class Transformer_ES_7_10_OS_2_11 implements Transformer {
    private int awarenessAttributeDimensionality;

    public Transformer_ES_7_10_OS_2_11(int awarenessAttributeDimensionality) {
        this.awarenessAttributeDimensionality = awarenessAttributeDimensionality;
    }

    public ObjectNode transformGlobalMetadata(ObjectNode root){
        return root;
    }

    public ObjectNode transformIndexMetadata(ObjectNode root){
        return root;
    }    
}
