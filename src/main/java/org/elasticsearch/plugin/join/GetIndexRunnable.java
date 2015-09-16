package org.elasticsearch.plugin.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dor on 18/08/15.
 */
public class GetIndexRunnable implements Runnable {

    private String indexName;
    private Map<String,Object> request;
    private Client esClient;
    private List<Map<String,Object>> index;
    private String exceptionString;
    private String Monitor;

    public GetIndexRunnable(String IndexName, Map<String, Object> request, Client esClient)
    {
        this.indexName=IndexName;
        this.request=request;
        this.esClient=esClient;
    }

    public void run()
    {
        long StartTime=System.currentTimeMillis();
        try {

            this.index = getHits(indexName, request, esClient);
        }
        catch (Exception exp)
        {
            this.index=null;
            this.exceptionString=exp.getMessage()+stackTraceToString(exp);
        }
        this.Monitor=String.valueOf(System.currentTimeMillis()-StartTime);
    }

    public String GetException()
    {
        return this.exceptionString;
    }

    public List<Map<String,Object>> GetIndex ()
    {
     if(this.index==null)
     {
         return null;
     }
        else
         return this.index;
    }

    public String GetMonitor ()
    {
        return this.Monitor;
    }

    private List<Map<String, Object>> getHits(String IndexName, Map<String, Object> request, Client esClient) throws Exception{

        ObjectMapper mapper = new ObjectMapper();
        String query  = Common.EMPTY_JSON;
        String filter = Common.EMPTY_JSON;
        String[] IndexNameSplit = null;
        String IndexType ="_all";
        if (((Map<String, Object>) request.get(Common.FILTERED)).containsKey(Common.QUERY)) {
            query = mapper.writeValueAsString(((Map<String, Object>) request.get(Common.FILTERED)).get(Common.QUERY));
        }
        if (((Map<String, Object>) request.get(Common.FILTERED)).containsKey(Common.FILTER)) {
            filter = mapper.writeValueAsString(((Map<String, Object>) request.get(Common.FILTERED)).get(Common.FILTER));
        }
        IndexNameSplit=indexName.split("/");
        IndexName=IndexNameSplit[0];
        SearchResponse responseA;
         if (IndexNameSplit.length==2)
        {
            IndexType=IndexNameSplit[1];
            responseA = esClient.prepareSearch(IndexName).setQuery(QueryBuilders.wrapperQuery(query)).setPostFilter(FilterBuilders.wrapperFilter(filter)).setSize(200000).setTypes(IndexType).execute().actionGet();
        }
        else {
              responseA = esClient.prepareSearch(IndexName).setQuery(QueryBuilders.wrapperQuery(query)).setPostFilter(FilterBuilders.wrapperFilter(filter)).setSize(200000).execute().actionGet();
         }

        List<Map<String, Object>> firstList = new ArrayList<Map<String, Object>>();

        for (SearchHit hit : responseA.getHits().getHits()) {
            firstList.add(hit.sourceAsMap());
        }
        return firstList;
    }
    private String stackTraceToString(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
}
