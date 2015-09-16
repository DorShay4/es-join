package org.elasticsearch.plugin.join;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import static org.elasticsearch.rest.RestStatus.*;

public class JoinRestHandler extends BaseRestHandler {

    @Inject
    public JoinRestHandler(Settings settings, RestController restController, Client esClient) {
        super(settings, restController, esClient);
        restController.registerHandler(RestRequest.Method.GET, "/_geojoin", this);
        restController.registerHandler(RestRequest.Method.POST, "/_geojoin", this);
        restController.registerHandler(RestRequest.Method.PUT, "/_geojoin", this);
        restController.registerHandler(RestRequest.Method.GET, "/_join", this);
        restController.registerHandler(RestRequest.Method.POST, "/_join", this);
        restController.registerHandler(RestRequest.Method.PUT, "/_join", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client esClient) {


        long BegiingTime = System.currentTimeMillis();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> carMap;

        try {
            carMap = mapper.readValue(request.content().toUtf8(), new TypeReference<Map<String, Object>>() {
            });

            if (carMap.containsKey(Common.FIRST_INDEX) && carMap.containsKey(Common.FIRST_QUERY) && carMap.containsKey(Common.SECOND_INDEX) && carMap.containsKey(Common.SECOND_QUERY)) {


                StringBuilder Monitor = new StringBuilder();


                //Get query from json
                String FirstIndex = carMap.get(Common.FIRST_INDEX).toString();

                Map<String, Object> FirstQuery = (Map<String, Object>) carMap.get(Common.FIRST_QUERY);

                String SecondIndex = carMap.get(Common.SECOND_INDEX).toString();
                Map<String, Object> SecondQuery = (Map<String, Object>) carMap.get(Common.SECOND_QUERY);

                //Start new thread to query es
                GetIndexRunnable firstGetIndex = new GetIndexRunnable(FirstIndex, FirstQuery, esClient);
                Thread firstObject = new Thread(firstGetIndex);
                firstObject.start();

                GetIndexRunnable secondGetIndex = new GetIndexRunnable(SecondIndex, SecondQuery, esClient);
                Thread secondObject = new Thread(secondGetIndex);
                secondObject.start();

                //Wait for the two query
                firstObject.join();
                secondObject.join();

                List<Map<String, Object>> firstList = firstGetIndex.GetIndex();
                List<Map<String, Object>> secondList = secondGetIndex.GetIndex();

                if (firstList == null) {
                    channel.sendResponse(new BytesRestResponse(EXPECTATION_FAILED, firstGetIndex.GetException()));

                }
                if (secondList == null) {
                    channel.sendResponse(new BytesRestResponse(EXPECTATION_FAILED, secondGetIndex.GetException()));

                }
                Monitor.append("{\"Get first index\":\"" + (firstGetIndex.GetMonitor()) + "\",\n");
                Monitor.append("\"first index count\":\""+ (firstGetIndex.GetIndex().size()) + "\",\n");
                Monitor.append("\"Get second index\":\"" + (secondGetIndex.GetMonitor()) + "\",\n");
                Monitor.append("\"second index count\":\""+ (secondGetIndex.GetIndex().size()) + "\",\n");


                if (request.path().endsWith("/_join/")) {
                    String returnJson = Join(firstList, secondList, carMap);
                    Monitor.append("\"Total time\":\"" + (System.currentTimeMillis() - BegiingTime) + "\",\n");
                    channel.sendResponse(new BytesRestResponse(OK,Monitor+ returnJson));

                } else {
                    String returnJson = GeoJoin(firstList, secondList, carMap);
                    Monitor.append("\"Total time\":\"" + (System.currentTimeMillis() - BegiingTime) + "\",\n");

                    channel.sendResponse(new BytesRestResponse(OK, Monitor+returnJson));

                }


            } else {
                channel.sendResponse(new BytesRestResponse(EXPECTATION_FAILED, "wrong json format :{\n" +
                        "    \"first_query\" : \"query1\",\n" +
                        "    \"first_index\" : \"index1\",\n" +
                        "    \"second_query\": \"query2\",\n" +
                        "    \"second_index\": \"index2\",\n" +
                        "    \"return_limit\": \"optional\",\n" +
                        "    \"QueryField\" : \"[{\"firstFiled\":\"first Column\",\n" +
                        "                     \"secondFiled\":\"second Column\",\n" +
                        "                     \"distance\"   : \"in minute,to date join\"}]\n" +
                        "}"));
            }


        } catch (Exception exp) {
            channel.sendResponse(new BytesRestResponse(OK, "exp: " + exp.getMessage() + stackTraceToString(exp)));
        }

    }

    private String GeoJoin(List<Map<String, Object>> firstList,List<Map<String, Object>> secondList,Map<String, Object> carMap) {

        StringBuilder Monitor = new StringBuilder();
        //long BegiingTime =System.currentTimeMillis();
        long StartTime ;
        long EndTime;
        List<ReturnObj> returnList;
        String returnAsJson="";

        StartTime = System.currentTimeMillis();
        GeoJoin joinObject = new GeoJoin();

        if (carMap.containsKey(Common.PARALLEL) && Integer.parseInt(carMap.get(Common.PARALLEL).toString()) > 1)
            joinObject.PARALLEL_SIZE = Integer.parseInt(carMap.get(Common.PARALLEL).toString());

        if(carMap.containsKey(Common.NODE_SIZE_NAME))
            Common.NODE_SIZE= Double.parseDouble(carMap.get(Common.NODE_SIZE_NAME).toString());

        if(carMap.containsKey(Common.RANGE))
            Common.RANGE_VALUE =Integer.parseInt(carMap.get(Common.RANGE).toString());

        //Send the two Query to join
        returnList = joinObject.JoinGeometries(firstList, secondList, Common.JOIN_COLUMN, Common.JOIN_COLUMN);

        EndTime = System.currentTimeMillis();

        Monitor.append("\"GeoJoin\":\"" + (EndTime - StartTime) + "\",\n");
        Monitor.append(joinObject.monitor);
        //Monitor.append("\"Total time\":\"" + (System.currentTimeMillis() - BegiingTime) + "\",\n");
        Monitor.append("\"Parallel\":\"" + joinObject.PARALLEL_SIZE + "\",");
        Monitor.append("\"Total hits\":\"" + returnList.size() + "\",");

        //convert to list to json,syntax("P:"[{json},{json}],"J":[{json},{json}])
        if (carMap.containsKey(Common.RETURN_LIMIT)) {
            returnAsJson = ListReturnObjectToJson(returnList, Integer.parseInt(carMap.get(Common.RETURN_LIMIT).toString()));
        } else {
            returnAsJson = ListReturnObjectToJson(returnList, 1000);
        }

        return  Monitor + "\"hits\":" + returnAsJson + "}";

    }
    private String Join(List<Map<String, Object>> firstList,List<Map<String, Object>> secondList,Map<String, Object> carMap) throws Exception {

        StringBuilder Monitor = new StringBuilder();
        String returnAsJson = "";
        long StartTime = System.currentTimeMillis();
        List<ReturnObj> JoinList = new ArrayList<ReturnObj>();
        String BIG_TABLE_COLUMN,SMALL_TABLE_COLUMN;

        List<ReturnObj> ReturnJoinTable = new ArrayList<ReturnObj>();
        int parallel=0;

        if (firstList.size() >= secondList.size()) {
            JoinList.add(new ReturnObj(firstList, secondList));
            BIG_TABLE_COLUMN=Common.FIRST_COLUMN;
            SMALL_TABLE_COLUMN=Common.SECOND_COLUMN;

        } else {
            JoinList.add(new ReturnObj(secondList,firstList ));
            BIG_TABLE_COLUMN=Common.SECOND_COLUMN;
            SMALL_TABLE_COLUMN=Common.FIRST_COLUMN;
        }

        if (carMap.containsKey(Common.QUERY_FIELD)) {
            List<Map<String, Object>> fieldMap = (List<Map<String, Object>>) carMap.get(Common.QUERY_FIELD);

            if(carMap.containsKey(Common.PARALLEL))
                parallel=(Integer)carMap.get(Common.PARALLEL);

            //field syntax "QueryField":[{"firstFiled":"firstcolumn","secondFiled":"secondColumn"}]
            for (Map<String, Object> curField : fieldMap) {
                if (curField.containsKey(Common.FIRST_COLUMN) && curField.containsKey(Common.SECOND_COLUMN)) {

                    ColumnCompare colCompare = new ColumnCompare();
                    colCompare.columnCompareIndexerA = curField.get(BIG_TABLE_COLUMN).toString();
                    colCompare.columnCompareIndexerB = curField.get(SMALL_TABLE_COLUMN).toString();
                    colCompare.parallel=parallel;

                    if (curField.containsKey(Common.DISTANCE))
                        colCompare.distance = Integer.parseInt(curField.get(Common.DISTANCE).toString());
                    if(curField.containsKey(Common.SORT))
                    {
                        //Because this the first time we have only to objects
                        JoinList.get(0).primaryObject=Common.sortList(JoinList.get(0).primaryObject,colCompare.columnCompareIndexerA);
                        JoinList.get(0).joinObject=Common.sortList(JoinList.get(0).joinObject,colCompare.columnCompareIndexerB);
                        colCompare.isOrder = Boolean.parseBoolean(curField.get(Common.SORT).toString());
                    }
                    for (ReturnObj listObj : JoinList) {
                        ReturnJoinTable.addAll(Join.ParallePlan(listObj.primaryObject, listObj.joinObject, colCompare));
                        Monitor.append(colCompare.monitor);
                    }
                    JoinList.clear();
                    JoinList.addAll(ReturnJoinTable);
                    ReturnJoinTable.clear();
                }
            }
        }

        //convert to list to json,syntax("P:"{json},"J":[{json},{json}])
        if (carMap.containsKey(Common.RETURN_LIMIT)) {
            returnAsJson = ListReturnObjectToJson(JoinList, Integer.parseInt(carMap.get(Common.RETURN_LIMIT).toString()));
        } else {
            returnAsJson = ListReturnObjectToJson(JoinList, 1000);
        }
        Monitor.append("\"Join\":\"" + (System.currentTimeMillis() - StartTime) + "\",\n");
        Monitor.append("\"Total hits\":\"" + JoinList.size() + "\",\n");

        return Monitor + "\"hits\":" + returnAsJson+"}" ;
    }

    private String stackTraceToString(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append(element.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    private String ListReturnObjectToJson(List<ReturnObj> lsReturn,int returnLimit) {
        int counter=1;
        if(lsReturn.size()==0)
        {
            return ("[]");
        }
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();
        sb.append("[");
        try {
            for (ReturnObj obj : lsReturn) {
                sb.append("{\"p\":" + mapper.writeValueAsString(obj.primaryObject)+",");
                sb.append("\"j\":" + mapper.writeValueAsString(obj.joinObject)+"},");
                if(counter>=returnLimit)
                    break;
                counter++;
            }
            sb.deleteCharAt(sb.length()-1);
            sb.append("]");
            return sb.toString();
        } catch (Exception exp) {
            return "Faild to convert returnobj to string";
        }
    }
}
