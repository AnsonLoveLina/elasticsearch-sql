package org.elasticsearch.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ngw.BulkProcessorProxy;
import com.unboundid.util.json.JSONValue;
import jodd.util.StringUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.index.IndexAction;
import org.nlpcn.es4sql.index.InsertAction;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.AggregationQueryAction;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.QueryAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
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
