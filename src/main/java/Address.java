import java.util.Hashtable;

public class Address implements java.io.Serializable {
    int pagenum;
    String tablename;
    Hashtable<String, Object> actualvalues;
    Object clusteringkey;
   
    public Address(int pagenum, String tablename, Hashtable<String, Object> actualvalues , Object key) {
        this.pagenum = pagenum;
        this.tablename = tablename;
        this.actualvalues = actualvalues;
        this.clusteringkey = key;
    }
}
