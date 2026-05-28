package com.axt.cnsmr.sunat.val.injector;

import com.axteroid.sdk.documents.impl.DocumentInfoOpenSearchDI;
import com.axteroid.sdk.exceptions.InternalException;
import com.axteroid.sdk.exceptions.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.Operator;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.CountRequest;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.core.SearchRequest;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Pattern;

import static com.axteroid.sdk.utils.Util.trimToNull;
import static java.util.Objects.isNull;
import static org.apache.logging.log4j.util.Strings.isBlank;

public class ExtDocumentInfoOpSearchDI extends DocumentInfoOpenSearchDI {

    private static final String PAGE_PARAMETER = "page";
    private static final String PAGE_SIZE_PARAMETER = "page_size";
    private static final String ORDERING_PARAMETER = "ordering";
    private static final String NOT_EXISTS_PARAMETER = "not_exists";
    private static final String EXISTS_PARAMETER = "exists";
    private static final String TIME_ZONE_PARAMETER = "time_zone";
    private static final String DEFAULT_TIME_ZONE = "America/Lima";

    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 10000;

    private static final Logger LOGGER = LogManager.getLogger(ExtDocumentInfoOpSearchDI.class);
    private static final Pattern FIELD_PATTERN = Pattern.compile("^([\\w.]+?)(?:__(\\w+))?$");

    public Long getCount(Map<String, Deque<String>> parameters) {
        SearchRequest srb = getSearchRequestBuilder(parameters).build();
        CountRequest request = CountRequest.of(r -> r
                .index(this.getAliasName())
                .query(srb.query())
        );
        CountResponse response;
        try {
            response = getClient().count(request);
        } catch (IOException ex) {
            throw new InternalException(ex.getMessage(), ex);
        }
        return response.count();
    }

    private SearchRequest.Builder getSearchRequestBuilder(Map<String, Deque<String>> parameters) {

        SearchRequest.Builder srb = new SearchRequest.Builder();
        String timeZone = Optional.ofNullable(parameters.remove(TIME_ZONE_PARAMETER))
                .map(Deque::getFirst)
                .orElse(DEFAULT_TIME_ZONE);

        Deque<String> orderingParam = parameters.remove(ORDERING_PARAMETER);
        if (orderingParam != null) {
            List<SortOptions> sorts = new ArrayList<>();
            for (var ordered : orderingParam) {
                SortOrder sortOrder;
                String sortFieldname;
                if (ordered.startsWith("-")) {
                    sortOrder = SortOrder.Desc;
                    sortFieldname = ordered.substring(1);
                } else {
                    sortOrder = SortOrder.Asc;
                    sortFieldname = ordered;
                }
                if (!isBlank(sortFieldname)) {
                    sorts.add(SortOptions.of(b -> b.field(f -> f.field(sortFieldname).order(sortOrder))));
                }
            }
            if (!sorts.isEmpty()) {
                srb.sort(sorts);
            }
        }

        int from;
        int pageSize = Optional.ofNullable(parameters.remove(PAGE_SIZE_PARAMETER))
                .map(Deque::getFirst)
                .map(Integer::parseInt)
                .or(() -> Optional.of(DEFAULT_PAGE_SIZE))
                .filter(ps -> ps <= MAX_PAGE_SIZE)
                .orElse(MAX_PAGE_SIZE);

        Deque<String> pageParam = parameters.remove(PAGE_PARAMETER);
        if (pageParam != null) {
            int page;
            try {
                page = Integer.parseInt(pageParam.getFirst());
            } catch (NumberFormatException ex) {
                throw new ValidationException("'" + PAGE_PARAMETER + "' parameter must be an integer number");
            }
            if (page <= 0) {
                throw new ValidationException("'" + PAGE_PARAMETER + "' must be greater than 0");
            }
            from = (page - 1) * pageSize;
        } else {
            from = 0;
        }
        srb.from(from).size(pageSize);

        List<Query> mustNotQueryList = new ArrayList<>();
        Deque<String> notExistsParameter = parameters.remove(NOT_EXISTS_PARAMETER);
        if (notExistsParameter != null) {
            for (String notExistsField : notExistsParameter) {
                if (trimToNull(notExistsField) != null) {
                    mustNotQueryList.add(Query.of(qb -> qb.exists(eqb -> eqb.field(notExistsField))));
                } else {
                    throw new ValidationException("not_exists value can't be empty");
                }
            }
        }

        List<Query> mustQueryList = new ArrayList<>();
        Deque<String> existsParameter = parameters.remove(EXISTS_PARAMETER);
        if (existsParameter != null) {
            for (String existsField : existsParameter) {
                if (trimToNull(existsField) != null) {
                    mustQueryList.add(Query.of(qb -> qb.exists(eqb -> eqb.field(existsField))));
                } else {
                    throw new ValidationException("exists value can't be empty");
                }
            }
        }

        for (Map.Entry<String, Deque<String>> entry : parameters.entrySet()) {
            var key = entry.getKey();
            var matcher = FIELD_PATTERN.matcher(key);
            if (matcher.matches() && matcher.groupCount() == 2) {
                if (isNull(matcher.group(2))) {
                    List<Query> valuesQuery = new ArrayList<>();
                    for (String value : entry.getValue()) {
                        if (!value.isBlank()) {
                            valuesQuery.add(Query.of(q -> q.term(t -> t.field(key).value(FieldValue.of(value)))));
                        }
                    }
                    if (!valuesQuery.isEmpty()) {
                        mustQueryList.add(Query.of(q -> q.bool(b -> b.should(valuesQuery))));
                    }
                } else {
                    var fieldname = matcher.group(1);
                    var condition = matcher.group(2);
                    switch (condition) {
                        case "ne" -> {
                            for (String value : entry.getValue()) {
                                if (!value.isBlank()) {
                                    mustNotQueryList.add(Query.of(q -> q.term(t -> t.field(fieldname).value(FieldValue.of(value)))));
                                }
                            }
                        }
                        case "not_exists" ->
                                mustNotQueryList.add(Query.of(qb -> qb.exists(eqb -> eqb.field(fieldname))));
                        case "exists" -> mustQueryList.add(Query.of(qb -> qb.exists(eqb -> eqb.field(fieldname))));
                        default -> {
                            if (entry.getValue().size() != 1) {
                                throw new ValidationException(MessageFormat.format("Range condition ''{0}'' must appear exactly once", key));
                            }
                            var value = JsonData.of(entry.getValue().getFirst());
                            mustQueryList.add(Query.of(q ->
                                    switch (condition) {
                                        case "gte" -> q.range(rb -> rb.field(fieldname).gte(value).timeZone(timeZone));
                                        case "lte" -> q.range(rb -> rb.field(fieldname).lte(value).timeZone(timeZone));
                                        case "gt" -> q.range(rb -> rb.field(fieldname).gt(value).timeZone(timeZone));
                                        case "lt" -> q.range(rb -> rb.field(fieldname).lt(value).timeZone(timeZone));
                                        case "text", "contains" -> q.match(
                                                mb -> mb.field(fieldname)
                                                        .query(FieldValue.of(entry.getValue().getFirst()))
                                                        .fuzziness("AUTO")
                                                        .operator(Operator.And)
                                        );
                                        default -> throw new ValidationException(
                                                MessageFormat.format(
                                                        "Unknown condition ''{0}'' for field ''{1}''",
                                                        condition, fieldname)
                                        );
                                    }
                            ));
                        }
                    }
                }
            } else {
                throw new ValidationException(MessageFormat.format("Field ''{0}'' didn''t match", key));
            }
        }

        srb.query(q -> q.bool(
                bb -> bb.must(mustQueryList)
                        .mustNot(mustNotQueryList)
        ));
        return srb;
    }
}
