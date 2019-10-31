package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlElasticUpdateByQueryRequestBuilder implements SqlElasticRequestBuilder {
    UpdateByQueryRequestBuilder updateByQueryRequestBuilder;

    public SqlElasticUpdateByQueryRequestBuilder(UpdateByQueryRequestBuilder updateByQueryRequestBuilder) {
        this.updateByQueryRequestBuilder = updateByQueryRequestBuilder;
    }

    @Override
    public ActionRequest request() {
        return updateByQueryRequestBuilder.request();
    }

    @Override
    public String explain() {
        try {
            SearchRequestBuilder source = updateByQueryRequestBuilder.source();
            return source.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ActionResponse get() {

        return this.updateByQueryRequestBuilder.get();
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return updateByQueryRequestBuilder;
    }

}
