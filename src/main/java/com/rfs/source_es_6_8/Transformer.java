package com.rfs.source_es_6_8;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Transformer {
    public ObjectNode transformTemplateBody(ObjectNode templateBody);
    public ObjectNode transformIndexBody(ObjectNode indexBody);
}
