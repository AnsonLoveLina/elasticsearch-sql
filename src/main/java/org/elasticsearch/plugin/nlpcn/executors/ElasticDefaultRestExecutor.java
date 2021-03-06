package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponseContentListener;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.plugin.nlpcn.*;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.JoinRequestBuilder;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.io.IOException;
import java.util.Map;


public class ElasticDefaultRestExecutor implements RestExecutor {


    public ElasticDefaultRestExecutor() {
    }

    /**
     * Execute the ActionRequest and returns the REST response using the channel.
     * zhongshu-comment 第二个参数Map<String, String> params 并没有被使用
     */
    @Override
    public void execute(RestHighLevelClient client, Map<String, String> params, Action queryAction, RestChannel channel) throws Exception {
        //zhongshu-comment queryAction的使命结束了，交由SqlElasticRequestBuilder接力，SqlElasticRequestBuilder是es-sql自己定义的一个类，不是es原生api
        SqlElasticRequestBuilder requestBuilder = queryAction.explain();

        //zhongshu-comment 看这行，将QueryAction对象转为es查询对象，这个是重点了，到这步就已经成功将sql字符串转化为es查询请求了
        //zhongshu-comment ActionRequest是es的原生api
        ActionRequest request = requestBuilder.request();

        //zhongshu-comment 应该是分别对应6中QueryAction子类实现
        if (requestBuilder instanceof JoinRequestBuilder) { //zhongshu-comment 对应连接查询：ESJoinQueryAction
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            executor.sendResponse(channel);
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) { //zhongshu-comment 对应union查询：MultiQueryAction
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            sendDefaultResponse(executor.getHits(), channel);
        } else if (request instanceof SearchRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：DefaultQueryAction、AggregationQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：SqlElasticSearchRequestBuilder
            client.searchAsync((SearchRequest) request, RequestOptions.DEFAULT, new RestStatusToXContentListener<>(channel));
        } else if (request instanceof DeleteByQueryRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：DeleteQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：SqlElasticDeleteByQueryRequestBuilder
            requestBuilder.getBuilder().execute(new BulkIndexByScrollResponseContentListener(channel, Maps.newHashMap()));
        } else if (request instanceof GetIndexRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：ShowQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：是一个匿名内部类，跳进去queryAction.explain()看
            requestBuilder.getBuilder().execute(new GetIndexRequestRestListener(channel, (GetIndexRequest) request));
        } else if (request instanceof SearchScrollRequest) {
            client.scrollAsync((SearchScrollRequest) request, RequestOptions.DEFAULT, new RestStatusToXContentListener<>(channel));
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }
    }

    @Override
    public String execute(RestHighLevelClient client, Map<String, String> params, Action queryAction) throws Exception {

        SqlElasticRequestBuilder requestBuilder = queryAction.explain();
        ActionRequest request = requestBuilder.request();

        if (requestBuilder instanceof JoinRequestBuilder) {
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (request instanceof SearchRequest) {
            SearchResponse response = client.search((SearchRequest) request, RequestOptions.DEFAULT);
            return response.toString();
        } else if (request instanceof DeleteByQueryRequest) {
            return requestBuilder.get().toString();
        } else if (request instanceof GetIndexRequest) {
            return requestBuilder.getBuilder().execute().actionGet().toString();
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }

    }

    private void sendDefaultResponse(SearchHits hits, RestChannel channel) {
        try {
            String json = ElasticUtils.hitsAsStringResult(hits, new MetaSearchResult());
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, json);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
