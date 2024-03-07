package com.rfs;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Transformer {
    public ObjectNode transformTemplateBody(ObjectNode templateBody);
    public ObjectNode transformIndexBody(ObjectNode indexBody);
}
