package org.elasticsearch.plugin.nlpcn.executors;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestChannel;
import org.nlpcn.es4sql.Action;

import java.util.Map;

/**
 * Created by Eliran on 26/12/2015.
 */
public interface RestExecutor {
    public void execute(RestHighLevelClient client, Map<String, String> params, Action queryAction, RestChannel channel) throws Exception;

    public String execute(RestHighLevelClient client, Map<String, String> params, Action queryAction) throws Exception;
}
