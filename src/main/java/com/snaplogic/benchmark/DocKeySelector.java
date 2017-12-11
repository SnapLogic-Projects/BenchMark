package com.snaplogic.benchmark;


import com.snaplogic.Document;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

public class DocKeySelector implements KeySelector<Document, String> {
    @Override
    public String getKey(Document document) throws Exception {
        return (String) ((Map<String, Object>) document.get()).get("0");
    }
}
