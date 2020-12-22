package org.nlpcn.es4sql;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;
import com.alibaba.druid.sql.parser.ParserException;


public class SearchDao {

	private static final Set<String> END_TABLE_MAP = new HashSet<>();

	static {
		END_TABLE_MAP.add("limit");
		END_TABLE_MAP.add("order");
		END_TABLE_MAP.add("where");
		END_TABLE_MAP.add("group");

	}

	private RestHighLevelClient client = null;


	public SearchDao(RestHighLevelClient client) {
		this.client = client;
	}

    public RestHighLevelClient getClient() {
        return client;
    }

    /**
     * Prepare action And transform sql
     * into ES ActionRequest
     *
     * @param sql SQL query to execute.
     * @return ES request
     * @throws SqlParseException
     */
    public Action explain(String sql) throws SqlParseException, SQLFeatureNotSupportedException {
        try {
            return ESActionFactory.create(client, sql);
        } catch (ParserException | SqlParseException pe) {
            throw new SqlParseException(sql, pe);
        }
    }


}
