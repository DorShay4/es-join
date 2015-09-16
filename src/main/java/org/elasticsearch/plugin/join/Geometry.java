package org.elasticsearch.plugin.join;



import java.util.Map;

public class Geometry {
    public Map<String, Object> origObj;
    public Rect rectangle;

    public Geometry(Rect _rect, Map<String, Object> _obj)
    {
        this.origObj = _obj;
        this.rectangle = _rect;
    }
}
