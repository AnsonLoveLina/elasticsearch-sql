package org.nlpcn.es4sql.query;

import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.nlpcn.es4sql.Action;
import org.nlpcn.es4sql.domain.Query;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to
 * SearchRequestBuilder (Represents ES query)
 */
public abstract class QueryAction implements Action {

    protected org.nlpcn.es4sql.domain.Query query;

    public String[] getIndexArr() {
        return query.getIndexArr();
    }

    public String[] getTypeArr() {
        return query.getTypeArr();
    }

    public QueryAction(Query query) {
        this.query = query;
    }

    protected void updateRequestWithStats(Select select, SearchRequest request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.STATS && hint.getParams() != null && 0 < hint.getParams().length) {
                request.source().stats(Lists.newArrayList(Arrays.stream(hint.getParams()).map(Object::toString).toArray(String[]::new)));
            }
        }
    }

    protected void updateRequestWithCollapse(Select select, SearchRequest request) throws SqlParseException {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.COLLAPSE && hint.getParams() != null && 0 < hint.getParams().length) {
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hint.getParams()[0].toString())) {
                    request.source().collapse(CollapseBuilder.fromXContent(parser));
                } catch (IOException e) {
                    throw new SqlParseException("could not parse collapse hint: " + e.getMessage());
                }
            }
        }
    }

    protected void updateRequestWithPostFilter(Select select, SearchRequest request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.POST_FILTER && hint.getParams() != null && 0 < hint.getParams().length) {
                request.source().postFilter(QueryBuilders.wrapperQuery(hint.getParams()[0].toString()));
            }
        }
    }

    protected void updateRequestWithIndexAndRoutingOptions(Select select, SearchRequest request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.IGNORE_UNAVAILABLE) {
                //saving the defaults from TransportClient search
                request.indicesOptions(IndicesOptions.fromOptions(true, false, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()));
            }
            if (hint.getType() == HintType.ROUTINGS) {
                Object[] routings = hint.getParams();
                String[] routingsAsStringArray = new String[routings.length];
                for (int i = 0; i < routings.length; i++) {
                    routingsAsStringArray[i] = routings[i].toString();
                }
                request.routing(routingsAsStringArray);
            }
        }
    }

    protected void updateRequestWithPreference(Select select, SearchRequest request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.PREFERENCE && hint.getParams() != null && 0 < hint.getParams().length) {
                request.preference(hint.getParams()[0].toString());
            }
        }
    }

    protected void updateRequestWithHighlight(Select select, SearchRequest request) {
        boolean foundAnyHighlights = false;
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.HIGHLIGHT) {
                HighlightBuilder.Field highlightField = parseHighlightField(hint.getParams());
                if (highlightField != null) {
                    foundAnyHighlights = true;
                    highlightBuilder.field(highlightField);
                }
            }
        }
        if (foundAnyHighlights) {
            request.source().highlighter(highlightBuilder);
        }
    }

    protected HighlightBuilder.Field parseHighlightField(Object[] params) {
        if (params == null || params.length == 0 || params.length > 2) {
            //todo: exception.
        }
        HighlightBuilder.Field field = new HighlightBuilder.Field(params[0].toString());
        if (params.length == 1) {
            return field;
        }
        Map<String, Object> highlightParams = (Map<String, Object>) params[1];

        for (Map.Entry<String, Object> param : highlightParams.entrySet()) {
            switch (param.getKey()) {
                case "type":
                    field.highlighterType((String) param.getValue());
                    break;
                case "boundary_chars":
                    field.boundaryChars(fromArrayListToCharArray((ArrayList) param.getValue()));
                    break;
                case "boundary_max_scan":
                    field.boundaryMaxScan((Integer) param.getValue());
                    break;
                case "force_source":
                    field.forceSource((Boolean) param.getValue());
                    break;
                case "fragmenter":
                    field.fragmenter((String) param.getValue());
                    break;
                case "fragment_offset":
                    field.fragmentOffset((Integer) param.getValue());
                    break;
                case "fragment_size":
                    field.fragmentSize((Integer) param.getValue());
                    break;
                case "highlight_filter":
                    field.highlightFilter((Boolean) param.getValue());
                    break;
                case "matched_fields":
                    field.matchedFields((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "no_match_size":
                    field.noMatchSize((Integer) param.getValue());
                    break;
                case "num_of_fragments":
                    field.numOfFragments((Integer) param.getValue());
                    break;
                case "order":
                    field.order((String) param.getValue());
                    break;
                case "phrase_limit":
                    field.phraseLimit((Integer) param.getValue());
                    break;
                case "post_tags":
                    field.postTags((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "pre_tags":
                    field.preTags((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "require_field_match":
                    field.requireFieldMatch((Boolean) param.getValue());
                    break;

            }
        }
        return field;
    }

    private char[] fromArrayListToCharArray(ArrayList arrayList) {
        char[] chars = new char[arrayList.size()];
        int i = 0;
        for (Object item : arrayList) {
            chars[i] = item.toString().charAt(0);
            i++;
        }
        return chars;
    }


    /**
     * Prepare the request, and return ES request.
     * zhongshu-comment 将sql字符串解析后的java对象，转换为es的查询请求对象
     *
     * @return ActionRequestBuilder (ES request)
     * @throws SqlParseException
     */
    public abstract SqlElasticRequestBuilder explain() throws SqlParseException;
}
