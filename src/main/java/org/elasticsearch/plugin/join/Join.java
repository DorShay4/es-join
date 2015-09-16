package org.elasticsearch.plugin.join;

import org.elasticsearch.common.joda.time.DateTime;

import java.util.*;

/**
 * Created by dor on 20/08/15.
 */
public class Join {
    public static List<ReturnObj> HashJoin(List<Map<String,Object>> firstIndex,List<Map<String,Object>> secondIndex,String CompareColumnNameA,String CompareColumnNameB)
    {
        HashMap<Object,List<Map<String,Object>>>  firstIndexHash = new HashMap<Object, List<Map<String,Object>>>() {
        };
        List<Map<String,Object>> sameObject;
        Object ColumnToComapre;
        for(Map<String,Object> currentA:firstIndex)
        {
            ColumnToComapre=currentA.get(CompareColumnNameA);
            if(!firstIndexHash.containsKey(ColumnToComapre)) {
                sameObject = new ArrayList<Map<String, Object>>();
                sameObject.add(currentA);
                firstIndexHash.put(ColumnToComapre, sameObject);
            }
            else
                firstIndexHash.get(ColumnToComapre).add(currentA);

        }

        List<ReturnObj> result = new ArrayList<ReturnObj>();


        for(Map<String,Object> currentB:secondIndex)
    {
        ColumnToComapre=currentB.get(CompareColumnNameB);
        if(firstIndexHash.containsKey(ColumnToComapre)) {

            result.add(new ReturnObj(currentB,firstIndexHash.get(ColumnToComapre)));
        }
    }
        return result;
    }


    public static List<ReturnObj> nestedLoopDate(List<Map<String,Object>> firstIndex,List<Map<String,Object>> secondIndex,String CompareColumnNameA,String CompareColumnNameB,long Distance)
    {
        DateTime ColumnToComapreA,ColumnToComapreB;

        //Distance in minute so we convert them to millsecond
        Distance=Distance*1000*60;
        List<ReturnObj> result = new ArrayList<ReturnObj>();
        for(Map<String,Object> currentA:firstIndex) {

            ColumnToComapreA=DateTime.parse(currentA.get(CompareColumnNameA).toString());
            List<Map<String,Object>> sameObject=new ArrayList<Map<String, Object>>();

            for (Map<String, Object> currentB : secondIndex) {

                ColumnToComapreB=DateTime.parse(currentB.get(CompareColumnNameB).toString());

                //check if the date are in range
                if((ColumnToComapreA.getMillis()-ColumnToComapreB.getMillis())<Distance&&(ColumnToComapreA.getMillis()-ColumnToComapreB.getMillis())>-Distance)
                {
                    sameObject.add(currentB);
                }
            }
            if(!sameObject.isEmpty())
            {
                result.add(new ReturnObj(currentA, sameObject));
            }
        }
        return result;
    }
    public static List<ReturnObj> MergeSort(List<Map<String,Object>> firstIndex,List<Map<String,Object>> secondIndex,String CompareColumnNameA,String CompareColumnNameB)
    {
        int firstIndexer=0;
        int secondIndexer=0;
        List<ReturnObj> returnList = new ArrayList<ReturnObj>();
        while(firstIndexer<firstIndex.size()&&secondIndexer<secondIndex.size())
        {
            Comparable currentA = (Comparable)firstIndex.get(firstIndexer).get(CompareColumnNameA);
            Comparable currentB = (Comparable)secondIndex.get(secondIndexer).get(CompareColumnNameB);

            int aCompareB =currentA.compareTo(currentB);

            //A bigger then B
            if(aCompareB>0)
            {
                secondIndexer++;
            }

            //A smaller then B
            else if(aCompareB<0)
            {
                firstIndexer++;
            }

            //A equal to B
            else
            {
                List<Map<String,Object>> objectToCompare  = new ArrayList<Map<String, Object>>();
                objectToCompare.add(secondIndex.get(secondIndexer));
                int tableBIndexNext = secondIndexer+1;

                //Find more object from B who equal to A
                while(tableBIndexNext<secondIndex.size()&&((Comparable)(secondIndex.get(tableBIndexNext)).get(CompareColumnNameB)).compareTo(currentA)==0)
                {
                    objectToCompare.add(secondIndex.get(tableBIndexNext));
                    tableBIndexNext++;
                }
                returnList.add(new ReturnObj(firstIndex.get(firstIndexer),objectToCompare));
                firstIndexer++;
                //Find more object from B who equal to A

            }
        }
        return returnList;
    }

    public static List<ReturnObj> NestedLoop(List<Map<String,Object>> firstIndex,List<Map<String,Object>> secondIndex,String CompareColumnNameA,String CompareColumnNameB)
    {
        List<ReturnObj> result = new ArrayList<ReturnObj>();

        for(Map<String,Object> currentObjFirstTable:firstIndex)
        {
            List<Map<String,Object>> returnList = new ArrayList<Map<String,Object>>();
            for(Map<String,Object> currentObjSecondTable:secondIndex)
            {
                if(currentObjFirstTable.get(CompareColumnNameA).equals(currentObjSecondTable.get(CompareColumnNameB)))
                    returnList.add(currentObjSecondTable);
            }
            if(!returnList.isEmpty())
                result.add(new ReturnObj(currentObjFirstTable,returnList));

        }
        return result;
    }
    /*public static List<ReturnObj> ExecuteJoin(List<Map<String,Object>> tableA,List<Map<String,Object>> tableB,List<ColumnCompare> parameterToCompare)
    {
        List<ReturnObj> JoinList = new ArrayList<ReturnObj>();
        JoinList.add(new ReturnObj(tableA,tableB));
        for(ColumnCompare colparam:parameterToCompare )
        {
            for (ReturnObj listObj:JoinList)
            {
                JoinList.addAll(ChoosePlan(listObj.primaryObject,listObj.joinObject,colparam));
            }
        }
    }*/

    public static List<ReturnObj> ChoosePlan(List<Map<String,Object>> tableA,List<Map<String,Object>> tableB,ColumnCompare param) {

        if (param.distance > 0) {
            return Join.nestedLoopDate(tableA, tableB, param.columnCompareIndexerA, param.columnCompareIndexerB, param.distance);
        }
        else if(param.isOrder)
            return Join.MergeSort(tableA,tableB,param.columnCompareIndexerA,param.columnCompareIndexerB);
        else
            return Join.HashJoin(tableA, tableB, param.columnCompareIndexerA, param.columnCompareIndexerB);

    }

    public static List<ReturnObj> ParallePlan(List<Map<String,Object>> tableA,List<Map<String,Object>> tableB,ColumnCompare param) {

        if (param.parallel == 0||tableA.size()<1000||tableB.size()<1000) {
            param.monitor="";
            return ChoosePlan(tableA, tableB, param);
        }
        List<ReturnObj> returnObjList = new ArrayList<ReturnObj>();
        Thread[] threadArray = new Thread[param.parallel];
        ChoosePlanRunnable[] runnableArray = new ChoosePlanRunnable[param.parallel];

        int searchGeomsSize = tableA.size() / param.parallel;
        int searchGeomMod = tableA.size() % param.parallel;

        try {
            //RUN ON PARALLEL AND SPLIT THE SEARCHGEOMS
            for (int i = 0; i < param.parallel; i++) {
                if (i == param.parallel - 1)
                    runnableArray[i] = new ChoosePlanRunnable(tableA.subList(searchGeomsSize * i, searchGeomsSize * (i + 1) + searchGeomMod), tableB, param);
                else
                    runnableArray[i] = new ChoosePlanRunnable(tableA.subList(searchGeomsSize * i, searchGeomsSize * (i + 1)), tableB, param);
                threadArray[i] = new Thread(runnableArray[i]);
                threadArray[i].start();
            }

            //WAIT FOR ALL THREAD TO STOP
            for (int i = 0; i < param.parallel; i++) {
                threadArray[i].join();
                returnObjList.addAll(runnableArray[i].GetList());
            }
            param.monitor = "\"parallel\":\"" + param.parallel + "\",\n";
            return returnObjList;

            //this.monitor += "\"FindIntersectionsRunnableParallel\":\"" + (System.currentTimeMillis() - start) + "\",";
        } catch (Exception exp) {
            param.monitor = "\"parallel\":\"failed\",\n";
            return ChoosePlan(tableA, tableB, param);
        }

    }

}

