package org.elasticsearch.plugin.join;


import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class QuadTreeNode {
    Rect m_bounds;

    public QuadTreeNode(Rect bounds)
    {
        this.m_bounds = bounds;
    }

    List<Geometry> m_contents = new ArrayList<Geometry>();
    List<QuadTreeNode> m_nodes = new ArrayList<QuadTreeNode>();

    public List<Geometry> Contents()
    {
        return m_contents;
    }

    public Rect Bounds()
    {
        return m_bounds;
    }

    public List<QuadTreeNode> Nodes()
    {
        return m_nodes;
    }

    public int Count()
    {
        return this.m_contents.size();
    }

    public List<Geometry> SubTreeContents()
    {
        List<Geometry> results = new ArrayList<Geometry>();

        for(Iterator<QuadTreeNode> i = m_nodes.iterator(); i.hasNext(); ) {
            QuadTreeNode item = i.next();

            results.addAll(item.SubTreeContents());
        }

        results.addAll(this.Contents());
        return results;
    }

    public List<Geometry> Query(Rect queryArea)
    {
        List<Geometry> results = new ArrayList<Geometry>();

        for(Iterator<Geometry> i = this.Contents().iterator(); i.hasNext(); ) {
            Geometry item = i.next();

            if (queryArea.IntersectsWith(item.rectangle)) {
                results.add(item);
            }
        }

        for(Iterator<QuadTreeNode> i = this.m_nodes.iterator(); i.hasNext(); ) {
            QuadTreeNode node = i.next();

            if(node.Bounds().Contains(queryArea))
            {
                results.addAll(node.Query(queryArea));
                break;
            }

            if(queryArea.Contains(node.Bounds()))
            {
                results.addAll(node.SubTreeContents());
            }

            if(node.Bounds().IntersectsWith(queryArea))
            {
                results.addAll(node.Query(queryArea));
            }
        }

        return results;
    }

    public void Insert(Geometry item)
    {
        if(m_nodes.size() == 0)
        {
            CreateSubNodes();
        }

        for(Iterator<QuadTreeNode> i = this.m_nodes.iterator(); i.hasNext(); ) {
            QuadTreeNode node = i.next();

            if (node.Bounds().Contains(item.rectangle)) {
                node.Insert(item);
                return;
            }
        }

        this.Contents().add(item);
    }

    public void CreateSubNodes()
    {
        if((m_bounds.Height * m_bounds.Width) <= Common.NODE_SIZE)
        {
            return;
        }

        double halfWidth = (m_bounds.Width / 2f);
        double halfHeight = (m_bounds.Height / 2f);

        m_nodes.add(new QuadTreeNode(new Rect(m_bounds.X, m_bounds.Y, halfWidth, halfHeight)));
        m_nodes.add(new QuadTreeNode(new Rect(m_bounds.X,m_bounds.Y - m_bounds.Height, halfWidth, halfHeight)));
        m_nodes.add(new QuadTreeNode(new Rect(m_bounds.X + halfWidth, m_bounds.Y, halfWidth, halfHeight)));
        m_nodes.add(new QuadTreeNode(new Rect(m_bounds.X + halfWidth, m_bounds.Y - halfHeight, halfWidth, halfHeight)));
    }
}
