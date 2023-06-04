import java.io.Serializable;

public class PageInfo implements Serializable{
    String clstkey;
    Object min;
    Object max;
    int capacity; 

    public PageInfo(String clstkey, Object min, Object max) {
        this.clstkey = clstkey;
        this.min = min;
        this.max = max;
        this.capacity = 0;

    }

}