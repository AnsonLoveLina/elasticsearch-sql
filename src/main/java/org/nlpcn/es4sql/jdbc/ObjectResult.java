package org.nlpcn.es4sql.jdbc;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ObjectResult {
    private final List<String> headers;
    private final List<List<Object>> lines;

    public ObjectResult(List<String> headers, List<List<Object>> lines) {
        this.headers = headers;
        this.lines = lines;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<Object>> getLines() {
        return lines;
    }

    public static ObjectResult emptyObjectResult(){
        return new ObjectResult(Lists.newArrayList(),Lists.newArrayList(Lists.newArrayList()));
    }
}
