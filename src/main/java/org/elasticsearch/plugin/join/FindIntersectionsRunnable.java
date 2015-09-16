package org.elasticsearch.plugin.join;

import java.util.List;

/**
 * Created by dor on 17/08/15.
 */
public class FindIntersectionsRunnable implements Runnable {
    private List<Geometry> searchGeoms;
    private QuadTree treeIndex;
    private List<ReturnObj>  returnObjList;

    public FindIntersectionsRunnable(List<Geometry> searchGeoms, QuadTree treeIndex) {
        this.searchGeoms = searchGeoms;
        this.treeIndex = treeIndex;
    }

    public void run() {
        int searchGeomsSize = searchGeoms.size();
        returnObjList = GeoJoin.FindIntersections(searchGeoms, treeIndex);

    }
    public  List<ReturnObj> GetList ()
    {
        return returnObjList;
    }
}


