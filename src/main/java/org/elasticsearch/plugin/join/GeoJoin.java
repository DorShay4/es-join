package org.elasticsearch.plugin.join;

import java.util.*;

public class GeoJoin {
    public  int PARALLEL_SIZE =10;
    public Rect TotalArea;
    public static String monitor;
    public GeoJoin()
    {
        TotalArea = new Rect(10,10,100,100);
    }

    public GeoJoin(Rect totalArea)
    {
        this.TotalArea = totalArea;
    }

    private QuadTree CreateQtreeIndex(List<Geometry> geoms, Rect area)
    {
        QuadTree treeIndex = new QuadTree(area);

        for(Iterator<Geometry> i = geoms.iterator(); i.hasNext(); ) {
            Geometry curShape = i.next();

            treeIndex.Insert(curShape);
        }

        return treeIndex;
    }

    private List<Geometry> createGeomsFromMap(List<Map<String,Object>> resultTable, String rectColumn)
    {
        List<Geometry> geoms = new ArrayList<Geometry>();
        Geometry currentGeom=null;
        for(Iterator<Map<String,Object>> i = resultTable.iterator(); i.hasNext(); ) {
            Map<String, Object> curObj = i.next();

            currentGeom=createGeomFromRect(curObj, rectColumn);

            if(currentGeom!=null) {
                geoms.add(currentGeom);
            }
        }
        return geoms;
    }

    private List<Geometry> createGeomsFromMapWithDistance(List<Map<String,Object>> resultTable, String rectColumn)
    {
        List<Geometry> geoms = new ArrayList<Geometry>();
        Geometry currentGeom=null;
        for(Iterator<Map<String,Object>> i = resultTable.iterator(); i.hasNext(); ) {
            Map<String, Object> curObj = i.next();

            currentGeom=createGeomFromRectWithDistance(curObj, rectColumn);

            if(currentGeom!=null) {
                geoms.add(currentGeom);
            }
        }
        return geoms;
    }

    private Geometry createGeomFromRectWithDistance(Map<String,Object> rectObj, String rectColumn)
    {
        if(rectObj.containsKey(rectColumn))
        {
            String[] coordinates = rectObj.get(rectColumn).toString().split(",");
            return new Geometry(new Rect(Double.parseDouble(coordinates[0]),
                    Double.parseDouble(coordinates[1]),Double.parseDouble(coordinates[2])+Common.RANGE_VALUE *Common.WIDTH_FACTOR,
                    Double.parseDouble(coordinates[3])+Common.RANGE_VALUE *Common.HEIGHT_FACTOR), rectObj);
        }

        return null;
    }

    private Geometry createGeomFromRect(Map<String,Object> rectObj, String rectColumn)
    {
        if(rectObj.containsKey(rectColumn))
        {
            String[] coordinates = rectObj.get(rectColumn).toString().split(",");
            return new Geometry(new Rect(Double.parseDouble(coordinates[0]),
                    Double.parseDouble(coordinates[1]),Double.parseDouble(coordinates[2]),
                            Double.parseDouble(coordinates[3])), rectObj);
        }

        return null;
    }

    public static List<ReturnObj> FindIntersections(List<Geometry> searchGeoms, QuadTree treeIndex) {
        List<ReturnObj> results = new ArrayList<ReturnObj>();

        List<Geometry> curIntersection;
        for (Iterator<Geometry> i = searchGeoms.iterator(); i.hasNext(); ) {
            Geometry curGeom = i.next();

            curIntersection = treeIndex.Query(curGeom.rectangle);
            if (curIntersection != null && curIntersection.size() > 0) {
                List<Map<String, Object>> intersections = new ArrayList<Map<String, Object>>();
                for (Iterator<Geometry> j = curIntersection.iterator(); j.hasNext(); ) {
                    Geometry curInter = j.next();

                    intersections.add(curInter.origObj);
                }
                results.add(new ReturnObj(curGeom.origObj, intersections));
            }
        }

        return results;
    }

    public  List<ReturnObj> JoinGeometries(List<Map<String, Object>> firstTable,
                                         List<Map<String, Object>> secondTable, String firstColumn,
                                         String secondColumn)
    {
        List<Geometry> IndexGeoms;
        List<Geometry> searchGeoms;
        long start1=System.currentTimeMillis();
        if(firstTable.size() <= secondTable.size())
        {
            IndexGeoms = Common.RANGE_VALUE >0?createGeomsFromMapWithDistance(firstTable,firstColumn):
            createGeomsFromMap(firstTable,firstColumn);
            searchGeoms = createGeomsFromMap(secondTable, secondColumn);
        }
        else
        {
            searchGeoms = createGeomsFromMap(firstTable,firstColumn);
            IndexGeoms = Common.RANGE_VALUE >0?createGeomsFromMapWithDistance(secondTable, secondColumn):
                    createGeomsFromMap(secondTable, secondColumn);
        }

        QuadTree treeIndex = this.CreateQtreeIndex(IndexGeoms, this.GetTotalArea(IndexGeoms));

        long End=System.currentTimeMillis();
        this.monitor="\"CreateQtreeIndex\":\""+(End-start1)+"\",";

        List<ReturnObj> returnObjList = new ArrayList<ReturnObj>();
        Thread[] threadArray = new Thread[PARALLEL_SIZE];
        FindIntersectionsRunnable[] runnableArray = new FindIntersectionsRunnable[PARALLEL_SIZE];

        int searchGeomsSize = searchGeoms.size()/ PARALLEL_SIZE;
        int searchGeomMod =searchGeoms.size()% PARALLEL_SIZE;
        long start =System.currentTimeMillis();

        try {
            //RUN ON PARALLEL AND SPLIT THE SEARCHGEOMS
            for (int i = 0; i < PARALLEL_SIZE; i++) {
                if(i== PARALLEL_SIZE -1)
                    runnableArray[i] = new FindIntersectionsRunnable(searchGeoms.subList(searchGeomsSize * i, searchGeomsSize * (i + 1)+searchGeomMod), treeIndex);
                else
                runnableArray[i] = new FindIntersectionsRunnable(searchGeoms.subList(searchGeomsSize * i, searchGeomsSize * (i + 1)), treeIndex);
                threadArray[i] = new Thread(runnableArray[i]);
                threadArray[i].start();
            }
            //WAIT FOR ALL THRED TO STOP
            for (int i = 0;i < PARALLEL_SIZE;i++) {
                threadArray[i].join();
                returnObjList.addAll(runnableArray[i].GetList());
            }
            this.monitor+="\"FindIntersectionsRunnableParallel\":\""+(System.currentTimeMillis()-start)+"\",";
        }
        //RUN WITHOUT PARALLEL
        catch (Exception exp)
        {
        returnObjList= this.FindIntersections(searchGeoms, treeIndex);
            this.monitor+="\"FindIntersectionsRunnableNoneParallel\":\""+(System.currentTimeMillis()-start)+"\",";
        }
        return returnObjList;
    }

    private Rect GetTotalArea(List<Geometry> geoms)
    {
        double minX = 100;
        double maxX = 0;
        double minY = 100;
        double maxY = 0;

        for (Iterator<Geometry> j = geoms.iterator(); j.hasNext(); ) {
            Rect curGeom = j.next().rectangle;

            if(curGeom.X > maxX)
            {
                maxX = curGeom.X;
            }
            if(curGeom.X < minX)
            {
                minX = curGeom.X;
            }

            if(curGeom.Y > maxY)
            {
                maxY = curGeom.Y;
            }
            if(curGeom.Y < minY) {
                minY = curGeom.Y;
            }
     }

        return new Rect(minX, maxY, maxX - minX + 1 , maxY - minY + 1);
    }
}
