package org.elasticsearch.plugin.join;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


public class Common {
    public static final String  FIRST_QUERY    ="first_query";
    public static final String  FIRST_INDEX    ="first_index";
    public static final String  SECOND_QUERY   ="second_query";
    public static final String  SECOND_INDEX   ="second_index";
    public static final String  JOIN_COLUMN    ="joinColumn";
    public static final String  FILTERED       ="filtered";
    public static final String  QUERY          ="query";
    public static final String  FILTER         ="filter";
    public static final String  EMPTY_JSON     ="{}";
    public static final String  SORT           ="sort";
    public static final String  PARALLEL       ="parallel";
    public static final String  RETURN_LIMIT   ="return_limit";
    public static final String  QUERY_FIELD    ="query_field";
    public static final String  FIRST_COLUMN   ="firstcolumn";
    public static final String  SECOND_COLUMN  ="secondcolumn";
    public static final String  DISTANCE       ="distance";
    public static final String  RANGE          ="range";
    public static final String  NODE_SIZE_NAME ="node_size";
    public static       Double  NODE_SIZE      =0.00001;
    public static       int     RANGE_VALUE    =0;
    public static final Double  WIDTH_FACTOR   =0.000009;
    public static final Double  HEIGHT_FACTOR  =0.000010617;


    public static List<Map<String,Object>> sortList (List<Map<String,Object>> listSort,final String  CompareColumnName)
    {
        Collections.sort(listSort, new Comparator<Map<String, Object>>() {
            public int compare(final Map<String, Object> o1, final Map<String, Object> o2) {
                return ((Comparable) o1.get(CompareColumnName)).compareTo((Comparable) o2.get(CompareColumnName));
            }
        });
        return listSort;
    }
}
