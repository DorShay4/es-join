package org.elasticsearch.plugin.join;

import java.util.List;
import java.util.Map;

/**
 * Created by dor on 03/09/15.
 */
public class ChoosePlanRunnable implements Runnable {
        private List<Map<String,Object>> splitTable;
        private List<Map<String,Object>> originalTable;
        private ColumnCompare param;
        private List<ReturnObj>  returnObjList;

        public ChoosePlanRunnable(List<Map<String,Object>> splitTable,List<Map<String,Object>> orignalTable,ColumnCompare param) {
            this.splitTable = splitTable;
            this.originalTable = orignalTable;
            this.param=param;
        }

        public void run() {
            int searchSize = splitTable.size()>originalTable.size()?splitTable.size():originalTable.size();

            if(searchSize<1000)
                returnObjList=Join.NestedLoop(splitTable, originalTable, param.columnCompareIndexerA, param.columnCompareIndexerB);

            else if (param.distance > 0) {
                returnObjList= Join.nestedLoopDate(splitTable, originalTable, param.columnCompareIndexerA, param.columnCompareIndexerB, param.distance);
            }
            else if(param.isOrder)
                returnObjList= Join.MergeSort(splitTable,originalTable
                        ,param.columnCompareIndexerA,param.columnCompareIndexerB);
            else
                returnObjList= Join.HashJoin(splitTable, originalTable, param.columnCompareIndexerA, param.columnCompareIndexerB);

        }
        public  List<ReturnObj> GetList ()
        {
            return returnObjList;
        }
}
