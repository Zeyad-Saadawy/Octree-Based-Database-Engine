import java.util.Hashtable;

public class Row implements java.io.Serializable{
    Hashtable<String, Object> values;

    public Row(Hashtable<String, Object> values) {
        this.values = values;
    }


    
}
