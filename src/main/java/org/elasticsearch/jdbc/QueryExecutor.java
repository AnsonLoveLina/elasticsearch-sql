package org.elasticsearch.jdbc;

import com.ngw.BulkProcessorProxy;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.index.IndexAction;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.AggregationQueryAction;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.ngw.SqlUtil.getRestClientBuilder;


public class QueryExecutor {

    private RestHighLevelClient restHighLevelClient;
    private final List<URI> uriList;
    private final Properties info;

    private BulkProcessor bulkProcessor;

    public QueryExecutor(String url, Properties info) throws SQLException {
        Object[] result = ESJDBCUtil.parseURL(url, info);
        this.uriList = (List<URI>) result[0];
        this.info = (Properties) result[1];
        buildClient();
    }

    public ObjectResult getObjectResult(boolean flat, String query, boolean includeScore, boolean includeType, boolean includeId) throws Exception {
        SearchDao searchDao = new SearchDao(this.restHighLevelClient);

        Action queryAction = searchDao.explain(query);
        SearchResponse searchResponse = restHighLevelClient.search((SearchRequest) queryAction.explain().request(), RequestOptions.DEFAULT);
        if (queryAction instanceof AggregationQueryAction) {
            return new ObjectResultsExtractor(includeScore, includeType, includeId, false, queryAction).extractResults(searchResponse.getAggregations(), flat);
        } else {
            return new ObjectResultsExtractor(includeScore, includeType, includeId, false, queryAction).extractResults(searchResponse, flat);
        }
    }

    public Action getAction(String query) throws Exception {
        SearchDao searchDao = new SearchDao(this.restHighLevelClient);

        Action action = searchDao.explain(query);
        return action;
    }

    public int add(IndexAction action) throws Exception {
        ActionRequest actionRequest = action.explain().request();
        if (actionRequest instanceof IndexRequest) {//新增
            bulkProcessor.add((IndexRequest) actionRequest);
            return action.getCount();
        } else {
            ActionRequest actionRequest1 = action.explain().request();
            if (actionRequest instanceof UpdateByQueryRequest || actionRequest instanceof UpdateRequest) {//修改
                BulkByScrollResponse actionResponse = restHighLevelClient.updateByQuery((UpdateByQueryRequest) actionRequest1, RequestOptions.DEFAULT);
                return Long.valueOf(actionResponse.getStatus().getUpdated()).intValue();
            } else {//删除
                BulkByScrollResponse actionResponse = restHighLevelClient.deleteByQuery((DeleteByQueryRequest) actionRequest1, RequestOptions.DEFAULT);
                return Long.valueOf(actionResponse.getStatus().getDeleted()).intValue();
            }
        }
    }

    public void commit() throws Exception {
        bulkProcessor.flush();
    }

    private void buildClient() throws SQLException {
        if (restHighLevelClient == null) {
            synchronized (this) {
                if (restHighLevelClient == null) {

                    String user = info.getProperty("user");
                    String password = info.getProperty("password");

                    RestClientBuilder restClientBuilder = getRestClientBuilder(user, password, uriList);

                    restHighLevelClient = new RestHighLevelClient(restClientBuilder);
                    this.bulkProcessor = BulkProcessorProxy.getBulkprocessor(restHighLevelClient);
                }
            }
        }
    }

    public List<URI> getUriList() {
        return uriList;
    }

    public RestHighLevelClient getClient() {
        return restHighLevelClient;
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    public void close() throws SQLException {

        // close elasticsearch client
        if (this.restHighLevelClient != null) {
            try {
                this.restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.restHighLevelClient = null;
        }
        try {
            bulkProcessor.awaitClose(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
    }

}
