package org.elasticsearch.plugin.join;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;

import java.util.List;
import java.util.Map;

/**
 * Created by dor on 12/08/15.
 */
public class JoinTest extends TestCase {

    public void testJoinGeometries() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

       try {
        String json1="[ {\n" +
                "               \"date\": \"2014-03-01T02:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +
                "               \"date\": \"2014-03-01T07:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +
                "               \"date\": \"2014-03-01T09:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +
                "               \"date\": \"2014-03-01T14:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +
                "               \"date\": \"2014-03-01T21:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +

                "               \"date\": \"2014-02-28T22:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +

                "               \"date\": \"2014-03-01T03:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +

                "               \"date\": \"2014-03-01T10:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +

                "               \"date\": \"2014-03-01T15:30:00.000Z\"\n" +
                "         },\n" +
                "         {\n" +

                "               \"date\": \"2014-02-28T23:30:00.000Z\"\n" +
                "         }]";
           String json2="[ {\n" +
                   "               \"date\": \"2014-03-01T02:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +
                   "               \"date\": \"2014-03-01T07:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-03-01T09:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +
                   "               \"date\": \"2014-03-01T14:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +
                   "               \"date\": \"2014-03-01T21:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-02-28T22:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-03-01T03:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-03-01T10:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-03-01T15:30:00.000Z\"\n" +
                   "         },\n" +
                   "         {\n" +

                   "               \"date\": \"2014-02-28T23:30:00.000Z\"\n" +
                   "         }]";
        String firstJson = "[{\"iD\":\"1\",\"joinColumn\":\"31,31,0.01,0.01\"},{\"iD\":\"1\",\"joinColumn\":\"31,31,0.01,0.01\"},{\"iD\":\"2\",\"joinColumn\":\"90,90,1,1\"},{\"iD\":\"3\",\"joinColumn\":\"41,41,0.01,0.01\"},{\"iD\":\"4\",\"joinColumn\":\"41,41,0.01,0.01\"},{\"iD\":\"5\",\"joinColumn\":\"41,41,0.01,0.01\"}]";
        String secondJson = "[{\"id\":\"1\",\"joinColumn\":\"31,31,0.01,0.01\"},{\"id\":\"2\",\"joinColumn\":\"31,31,0.01,0.01\"},{\"id\":\"3\",\"joinColumn\":\"31,31,0.01,0.01\"},{\"id\":\"4\",\"joinColumn\":\"41,41,0.01,0.01\"},{\"id\":\"5\",\"joinColumn\":\"41,41,0.01,0.01\"}]";
            List<Map<String, Object>> firstlist = mapper.readValue(firstJson, new TypeReference< List<Map<String, Object>> >() {
        });

            List<Map<String, Object>>  secondlist = mapper.readValue(secondJson, new TypeReference< List<Map<String, Object>> >() {
        });

          /* String test = "index/type";
           String test2 = "index";
           String[] a2=test2.split("/");
           String[] a1 =test.split("/");*/
          /* ColumnCompare a = new ColumnCompare();
           a.columnCompareIndexerA="iD";
           a.columnCompareIndexerB="id";
           a.isOrder=true;
           List<ReturnObj> returnList= Join.ChoosePlan(firstlist, secondlist, a);*/
           GeoJoin joinObject = new GeoJoin();
           Common.RANGE_VALUE =1;
            List<ReturnObj> returnList = joinObject.JoinGeometries(firstlist, secondlist, "joinColumn", "joinColumn");

            String one="0";
        }
        catch (Exception exp)
        {
String one="0";
        }
    }


}