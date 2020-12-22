package org.elasticsearch.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.index.IndexAction;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.query.AggregationQueryAction;
import org.nlpcn.es4sql.query.DefaultQueryAction;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ObjectResultUtil {


    public static ObjectResult getObjectResultBySearch(SearchResponse searchResponse) {
        searchResponse.getHits().getHits();
        return null;
    }

    private static Request getQueryRequest(String index, String type, String requestJson) {

        String method = "GET";
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("/").append(index);
        if (type != null) {
            stringBuilder.append("/").append(type);
        }
        stringBuilder.append("/_search");
        Request request = new Request(method, stringBuilder.toString());
        request.setJsonEntity(requestJson);
        return request;
    }

    private static JSONObject getJSONObject(String index, String type, String requestJson, RestClient restClient, Action action) {
        Request request = null;
        if (!(action instanceof IndexAction)) {
            request = getQueryRequest(index, type, requestJson);
        }
        try {
            Response response = restClient.performRequest(request);
            return JSON.parseObject(response.getEntity().getContent(), Charset.forName("UTF-8"), JSONObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ObjectResult getObjectResult(Action action, String index, String type, String requestJson, RestClient restClient) {
        JSONObject map = getJSONObject(index, type, requestJson, restClient, action);
        if (map == null) {
            return ObjectResult.emptyObjectResult();
        }
        List<String> headers = Lists.newArrayList();
        List<List<Object>> lines = Lists.newArrayList();
        if (action instanceof DefaultQueryAction) {
            JSONArray jsonArray = map.getJSONObject("hits").getJSONArray("hits");

            if (!jsonArray.isEmpty()) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject hits = jsonArray.getJSONObject(i);
                    JSONObject source = hits.getJSONObject("_source");
                    if (i == 0) {
                        headers.add("_id");
                        headers.addAll(source.keySet());
                    }
                    List line = Lists.newArrayList(hits.get("_id"));
                    line.addAll(source.values());
                    lines.add(line);
                }
            }

            return new ObjectResult(headers, lines);
        }
        if (action instanceof AggregationQueryAction) {
            JSONObject aggs = map.getJSONObject("aggregations");

            List<Object> line = Lists.newArrayList();
            lines.add(line);
            for (Map.Entry aggObj : aggs.entrySet()) {
                String columnName = (String) aggObj.getKey();
                headers.add(columnName + Util.BUCKS_NAME);
                JSONObject jsonObject = (JSONObject) aggObj.getValue();
                JSONArray jsonArray = jsonObject.getJSONArray("buckets");
                List<String> bucksHeaders = new ArrayList<>();
                List<List<Object>> buckline = Lists.newArrayList();

                if (!jsonArray.isEmpty()) {
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject hits = jsonArray.getJSONObject(i);
                        List line1 = Lists.newArrayList(hits.get("key"), hits.get("doc_count"));
                        buckline.add(line1);
                    }
                }
                List<List<Object>> bucksLines = new ArrayList<>();
                bucksHeaders.add(columnName + Util.KEY_NAME);
                bucksHeaders.add(columnName + Util.COUNT_NAME);
                ElasticSearchArray elasticSearchArray = new ElasticSearchArray(bucksHeaders, buckline);
                line.add(elasticSearchArray);
            }

            return new ObjectResult(headers, lines);
        }
        return ObjectResult.emptyObjectResult();
    }
}
