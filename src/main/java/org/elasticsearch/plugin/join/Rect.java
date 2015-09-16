package org.elasticsearch.plugin.join;

public class Rect {
    public double X;
    public double Y;
    public double Width;
    public double Height;

    public Rect(double _x, double _y, double _w,double _h)
    {
        this.X = _x;
        this.Y = _y;
        this.Width = _w;
        this.Height = _h;
    }

    public boolean IntersectsWith(Rect cmpRect)
    {
        if(!(cmpRect.X > this.X + this.Width || cmpRect.X + cmpRect.Width < this.X ||
                cmpRect.Y < this.Y - this.Height || cmpRect.Y - cmpRect.Height > this.Y ))
        {
            return true;
        }

        return false;
    }

    public boolean Contains(Rect cmpRect)
    {
        if(this.X <= cmpRect.X && this.X + this.Width >= cmpRect.X + cmpRect.Width &&
                this.Y >= cmpRect.Y && this.Y - this.Height <= cmpRect.Y - cmpRect.Height)
        {
            return true;
        }

        return false;
    }

}
