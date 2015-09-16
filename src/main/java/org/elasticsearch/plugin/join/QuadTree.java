package org.elasticsearch.plugin.join;


import java.util.List;

public class QuadTree {
    QuadTreeNode m_root;
    Rect m_rectangle;

    public QuadTree(Rect rectangle)
    {
        m_rectangle = rectangle;
        m_root = new QuadTreeNode(m_rectangle);
    }

    public int Count()
    {
        return m_root.Count();
    }

    public void Insert(Geometry item)
    {
        if(m_root.Bounds().Contains(item.rectangle))
        {
            m_root.Insert(item);
        }
    }

    public List<Geometry> Query(Rect area)
    {
        return m_root.Query(area);
    }
}
