package org.elasticsearch.plugin.nlpcn;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.elasticsearch.rest.*;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.Action;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;


public class RestSqlAction extends BaseRestHandler {

    private static final Logger LOGGER = LogManager.getLogger();

	public RestSqlAction(Settings settings, RestController restController) {
        super(settings);
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
	}

    @Override
    public String getName() {
        return "sql_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parser.mapStrings().forEach((k, v) -> request.params().putIfAbsent(k, v));
        } catch (IOException e) {
            LOGGER.warn("Please use json format params, like: {\"sql\":\"SELECT * FROM test\"}");
        }

        String sql = request.param("sql");

        if (sql == null) {
            sql = request.content().utf8ToString();
        }
        List<NodeInfo> nodeInfos = client.admin().cluster().prepareNodesInfo().get().getNodes();
        String user = client.settings().get("user");
        String password = client.settings().get("password");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));
        HttpHost[] httpHosts = new HttpHost[nodeInfos.size()];
        for (int i = 0; i < nodeInfos.size(); ++i) {
            try {
                httpHosts[i] = new HttpHost(InetAddress.getByName(nodeInfos.get(i).getTransport().getAddress().publishAddress().getAddress()), nodeInfos.get(i).getTransport().getAddress().publishAddress().getPort(), "http");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }).setFailureListener(new RestClient.FailureListener() {

                    @Override
                    public void onFailure(Node node) {
                        System.out.println("node = " + node);
                    }
                });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        try {
            SearchDao searchDao = new SearchDao(restHighLevelClient);
            Action queryAction = null;

            queryAction = searchDao.explain(sql);//zhongshu-comment 语法解析，将sql字符串解析为一个Java查询对象

            // TODO add unit tests to explain. (rest level?)
            if (request.path().endsWith("/_explain")) {
                final String jsonExplanation = queryAction.explain().explain();
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentType.JSON.mediaType(), jsonExplanation));
            } else {
                Map<String, String> params = request.params();

                //zhongshu-comment 生成一个负责用rest方式查询es的对象RestExecutor，返回的实现类是：ElasticDefaultRestExecutor
                RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
                final Action finalQueryAction = queryAction;
                //doing this hack because elasticsearch throws exception for un-consumed props
                Map<String, String> additionalParams = new HashMap<>();
                for (String paramName : responseParams()) {
                    if (request.hasParam(paramName)) {
                        additionalParams.put(paramName, request.param(paramName));
                    }
                }
                //zhongshu-comment restExecutor.execute()方法里会调用es查询的相关rest api
                //zhongshu-comment restExecutor.execute()方法的第1、4个参数是框架传进来的参数，第2、3个参数是可以自己生成的参数，所以要多注重一点
                //zhongshu-comment 默认调用的是ElasticDefaultRestExecutor这个子类
                //todo 这是什么语法：搜索java8 -> lambda表达式：https://blog.csdn.net/ioriogami/article/details/12782141
                return channel -> restExecutor.execute(restHighLevelClient, additionalParams, finalQueryAction, channel);
            }
        } catch (SqlParseException | SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format"));
        return responseParams;
    }
}