package org.elasticsearch.plugin.join;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

public class ReturnObj {
    List<Map<String,Object>> primaryObject;
    List<Map<String,Object>> joinObject;

    public ReturnObj(Map<String,Object> _primary,List<Map<String,Object>> _join)
    {
        this.primaryObject = new ArrayList<Map<String, Object>>();
        this.primaryObject.add(_primary);
        this.joinObject = _join;
    }

    public ReturnObj(List<Map<String,Object>> _primary,List<Map<String,Object>> _join)
    {
        this.primaryObject = _primary;
        this.joinObject = _join;
    }

}
