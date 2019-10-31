package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.nlpcn.es4sql.domain.Update;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.index.IndexAction;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.util.Collections;
import java.util.List;

public class UpdateQueryAction extends QueryAction implements IndexAction {

    private final Update update;
    private UpdateByQueryRequestBuilder request;

    public UpdateQueryAction(Client client, Update update) {
        super(client, update);
        this.update = update;
    }

    @Override
    public SqlElasticUpdateByQueryRequestBuilder explain() throws SqlParseException {
        this.request = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);

        setIndicesAndTypes();
        setWhere(update.getWhere());
        setItems(update.getItems());

        // maximum number of processed documents
        if (update.getRowCount() > -1) {
            request.size(update.getRowCount());
        }

        // set conflicts param
        updateRequestWithConflicts();

        SqlElasticUpdateByQueryRequestBuilder updateByQueryRequestBuilder = new SqlElasticUpdateByQueryRequestBuilder(request);
        return updateByQueryRequestBuilder;
    }

    private void setItems(List<SQLUpdateSetItem> items) {
        StringBuilder sb = new StringBuilder();
        for (SQLUpdateSetItem item : items) {
            sb.append("ctx._source.").append(item.getColumn()).append("=").append(item.getValue()).append(";\n");
        }
        request.script(new Script(ScriptType.INLINE,
                "painless",
                sb.toString(),
                Collections.emptyMap()));
    }


    /**
     * Set indices and types to the delete by query request.
     */
    private void setIndicesAndTypes() {

        UpdateByQueryRequest innerRequest = request.request();
        innerRequest.indices(query.getIndexArr());
        String[] typeArr = query.getTypeArr();
        if (typeArr != null) {
            innerRequest.getSearchRequest().types(typeArr);
        }
//		String[] typeArr = query.getTypeArr();
//		if (typeArr != null) {
//            request.set(typeArr);
//		}
    }


    /**
     * Create filters based on
     * the Where clause.
     *
     * @param where the 'WHERE' part of the SQL query.
     * @throws SqlParseException
     */
    private void setWhere(Where where) throws SqlParseException {
        if (where != null) {
            QueryBuilder whereQuery = QueryMaker.explan(where);
            request.filter(whereQuery);
        } else {
            request.filter(QueryBuilders.matchAllQuery());
        }
    }

    private void updateRequestWithConflicts() {
        for (Hint hint : update.getHints()) {
            if (hint.getType() == HintType.CONFLICTS && hint.getParams() != null && 0 < hint.getParams().length) {
                String conflicts = hint.getParams()[0].toString();
                switch (conflicts) {
                    case "proceed":
                        request.abortOnVersionConflict(false);
                        return;
                    case "abort":
                        request.abortOnVersionConflict(true);
                        return;
                    default:
                        throw new IllegalArgumentException("conflicts may only be \"proceed\" or \"abort\" but was [" + conflicts + "]");
                }
            }
        }
    }

    @Override
    public int getCount() {
        return update.getRowCount();
    }
}
