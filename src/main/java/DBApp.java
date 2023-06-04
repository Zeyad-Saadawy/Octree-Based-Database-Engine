import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File; // Import the File class
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class DBApp {

    public void init() {

    }

    public static void serializeTable(Table obj) throws DBAppException {
        try {
            FileOutputStream file = new FileOutputStream("src/main/resources/Data/" + obj.tableName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(obj);

            out.close();
            file.close();

        } catch (IOException e) {
            // Catch the IOException and throw a DBAppException with the same or a modified
            // message
            throw new DBAppException("Error in serializing table" + e.getMessage());
        }
    }

    public static Table deserializeTable(String TableName) throws DBAppException {
        Table obj = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/Data/" + TableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            obj = (Table) in.readObject();

            in.close();
            file.close();

        }

        catch (IOException e) {
            throw new DBAppException("IOException is caught" + e.getMessage());
        }

        catch (ClassNotFoundException ex) {
            throw new DBAppException("ClassNotFoundException is caught" + ex.getMessage());
        }

        return obj;
    }

    public static void createTable(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax) throws DBAppException {
        try {
            // meta-data file
            File metadataFile = new File("src/main/resources/metadata.csv");

            if (metadataFile.exists()) {
                // read existing metadata to check if table already exists
                BufferedReader br = new BufferedReader(new FileReader(metadataFile));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tableInfo = line.split(",");
                    if (tableInfo[0].equals(strTableName)) {
                        // table already exists
                        throw new DBAppException("Table with the same name already exists in the metadata.");
                    }
                }
                br.close();
            } else {
                // create new metadata file
                metadataFile.createNewFile();

            }

            // check for invalid data types
            for (String key : htblColNameType.keySet()) {
                String type = htblColNameType.get(key);
                if (!(type.equals("java.lang.Integer") || type.equals("java.lang.String")
                        || type.equals("java.lang.Double") || type.equals("java.util.Date"))) {
                    throw new DBAppException("Invalid data type");
                }
            }
            // check for invalid columns in min and max
            for (String key : htblColNameMin.keySet()) {
                if (!htblColNameType.containsKey(key)) {
                    throw new DBAppException("Column " + key + " in htblColNameMin does not exist in htblColNameType");
                }
            }

            for (String key : htblColNameMax.keySet()) {
                if (!htblColNameType.containsKey(key)) {
                    throw new DBAppException("Column " + key + " in htblColNameMax does not exist in htblColNameType");
                }
            }

            // check for missing columns in min
            for (String key : htblColNameType.keySet()) {
                if (!htblColNameMin.containsKey(key)) {
                    throw new DBAppException("Column " + key + " in htblColNameType does not exist in htblColNameMin");
                }
            }
            // check for missing columns in max
            for (String key : htblColNameType.keySet()) {
                if (!htblColNameMax.containsKey(key)) {
                    throw new DBAppException("Column " + key + " in htblColNameType does not exist in htblColNameMax");
                }
            }
            // check if min is null
            for (String key : htblColNameMin.keySet()) {
                if (htblColNameMin.get(key) == null) {
                    throw new DBAppException("Column " + key + " in htblColNameMin is null");
                }
            }
            // check if max is null
            for (String key : htblColNameMax.keySet()) {
                if (htblColNameMax.get(key) == null) {
                    throw new DBAppException("Column " + key + " in htblColNameMax is null");
                }
            }

            FileWriter myWriter = new FileWriter(metadataFile, true);
            // Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType,
            // min, max
            for (String key : htblColNameType.keySet()) {
                myWriter.write(strTableName + "," + key + "," + htblColNameType.get(key) + ","
                        + (key.equals(strClusteringKeyColumn) ? "True" : "False") + "," + null + "," + null + ","
                        + htblColNameMin.get(key) + "," + htblColNameMax.get(key) + "\n");
            }

            myWriter.close();

            Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                    htblColNameMax);

            // serialize the t object

            serializeTable(t);
        } catch (IOException e) {
            throw new DBAppException("Error in creating table" + e.getMessage());

        }
    }

    public static void insertIntoTable(String strTableName,
            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        // check if any of the values is null
        for (String key : htblColNameValue.keySet()) {
            if (htblColNameValue.get(key) == null) {
                throw new DBAppException("Column " + key + " in htblColNameValue is null");
            }
        }
        // 1 read metadata.csv ,check if table exits and get values of this table
        // 2 check row validity (data types and values are in range)
        // 3 insert the row in the table
        // 4 serialize the table

        BufferedReader CSVreader;
        String strClusteringKeyColumn = "";
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();

        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();

            // check if strTableName exists
            boolean tableExists = false;
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(strTableName)) {
                    tableExists = true;
                    if (parts[3].equals("True")) {
                        strClusteringKeyColumn = parts[1];
                    }

                    htblColNameType.put(parts[1], parts[2]);
                    htblColNameMin.put(parts[1], parts[6]);
                    htblColNameMax.put(parts[1], parts[7]);
                }
                line = CSVreader.readLine();
            }
            if (!tableExists) {
                CSVreader.close();
                throw new DBAppException("Table does not exist");
            }
            CSVreader.close();

        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());

        }

        // check if clustering key is included in htblColNameValue
        if (!htblColNameValue.containsKey(strClusteringKeyColumn)) {
            throw new DBAppException("Clustering key column not included in htblColNameValue");
        }
        // 2 check if the row has valid value types and values are in range
        for (String key : htblColNameValue.keySet()) {

            // check if column exists
            if (!htblColNameType.containsKey(key)) {
                throw new DBAppException("Column does not exist");
            }
            // value check
            if (htblColNameType.get(key).equals("java.lang.Integer")) {
                if (!(htblColNameValue.get(key) instanceof Integer)) {
                    throw new DBAppException("Invalid value type");
                }
                if ((int) htblColNameValue.get(key) < Integer.parseInt(htblColNameMin.get(key))
                        || (int) htblColNameValue.get(key) > Integer.parseInt(htblColNameMax.get(key))) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.String")) {
                if (!(htblColNameValue.get(key) instanceof String)) {
                    throw new DBAppException("Invalid value type");
                }
                if (((String) htblColNameValue.get(key)).compareTo(htblColNameMin.get(key)) < 0
                        || ((String) htblColNameValue.get(key)).compareTo(htblColNameMax.get(key)) > 0) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.Double")) {
                if (!(htblColNameValue.get(key) instanceof Double)) {
                    throw new DBAppException("Invalid value type");
                }
                if ((double) htblColNameValue.get(key) < Double.parseDouble(htblColNameMin.get(key))
                        || (double) htblColNameValue.get(key) > Double.parseDouble(htblColNameMax.get(key))) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.util.Date")) {
                if (!(htblColNameValue.get(key) instanceof Date)) {
                    throw new DBAppException("Invalid value type");
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                Date min;
                Date max;
                try {
                    min = formatter.parse(htblColNameMin.get(key));
                    max = formatter.parse(htblColNameMax.get(key));
                } catch (ParseException e) {
                    throw new DBAppException("Invalid date format" + e.getMessage());
                }
                if (((Date) htblColNameValue.get(key)).compareTo(min) < 0
                        || ((Date) htblColNameValue.get(key)).compareTo(max) > 0) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.Boolean")) {
                if (!(htblColNameValue.get(key) instanceof Boolean)) {
                    throw new DBAppException("Invalid value type");
                }
            } else {
                throw new DBAppException("Invalid value type");
            }
        }
        // 2 read the table file and deserialize it
        Table t = deserializeTable(strTableName);

        // check row has valid names for columns
        for (String key : htblColNameValue.keySet()) {
            if (!t.htblColNameType.keySet().contains(key)) {
                throw new DBAppException("Column does not exist");
            }
        }

        // 3 insert the row in the table
        Row r = new Row(htblColNameValue);
        try {
            t.insertIntoPage(r);
        } catch (IOException e) {
            throw new DBAppException("Error inserting row into page" + e.getMessage());
        }
        // 4 serialize the table
        serializeTable(t);
    }

    public static void updateTable(String strTableName,
            String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        // check from metadata if table exists
        BufferedReader CSVreader;
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        String strClusteringKeyColumn = "";
        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();

            // check if strTableName exists
            boolean tableExists = false;
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(strTableName)) {
                    tableExists = true;
                    htblColNameType.put(parts[1], parts[2]);
                    htblColNameMin.put(parts[1], parts[6]);
                    htblColNameMax.put(parts[1], parts[7]);
                }
                line = CSVreader.readLine();
            }
            if (!tableExists) {
                CSVreader.close();
                throw new DBAppException("Table does not exist");
            }
            CSVreader.close();

        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());

        }
        // 2 check if the row has valid value types and values are in range
        for (String key : htblColNameValue.keySet()) {
            // check if column exists
            if (!htblColNameType.containsKey(key)) {
                throw new DBAppException("Column does not exist");
            }
            // check if clusteringkey is updated
            if (key.equals(strClusteringKeyColumn)) {
                throw new DBAppException("Clustering key cannot be updated");
            }
            // check if clusteringkey value is null
            if (strClusteringKeyValue == null) {
                throw new DBAppException("Clustering key value cannot be null");
            }
            // value check
            if (htblColNameType.get(key).equals("java.lang.Integer")) {
                if (!(htblColNameValue.get(key) instanceof Integer)) {
                    throw new DBAppException("Invalid value type");
                }
                if ((int) htblColNameValue.get(key) < Integer.parseInt(htblColNameMin.get(key))
                        || (int) htblColNameValue.get(key) > Integer.parseInt(htblColNameMax.get(key))) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.String")) {
                if (!(htblColNameValue.get(key) instanceof String)) {
                    throw new DBAppException("Invalid value type");
                }
                if (((String) htblColNameValue.get(key)).compareTo(htblColNameMin.get(key)) < 0
                        || ((String) htblColNameValue.get(key)).compareTo(htblColNameMax.get(key)) > 0) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.Double")) {
                if (!(htblColNameValue.get(key) instanceof Double)) {
                    throw new DBAppException("Invalid value type");
                }
                if ((double) htblColNameValue.get(key) < Double.parseDouble(htblColNameMin.get(key))
                        || (double) htblColNameValue.get(key) > Double.parseDouble(htblColNameMax.get(key))) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.util.Date")) {
                if (!(htblColNameValue.get(key) instanceof Date)) {
                    throw new DBAppException("Invalid value type");
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                Date min;
                Date max;
                try {
                    min = formatter.parse(htblColNameMin.get(key));
                    max = formatter.parse(htblColNameMax.get(key));
                } catch (ParseException e) {
                    throw new DBAppException("Invalid date format" + e.getMessage());
                }
                if (((Date) htblColNameValue.get(key)).compareTo(min) < 0
                        || ((Date) htblColNameValue.get(key)).compareTo(max) > 0) {
                    throw new DBAppException("Value is out of range");
                }
            } else if (htblColNameType.get(key).equals("java.lang.Boolean")) {
                if (!(htblColNameValue.get(key) instanceof Boolean)) {
                    throw new DBAppException("Invalid value type");
                }
            } else {
                throw new DBAppException("Invalid value type");
            }
        }

        Table t = deserializeTable(strTableName);
        // check row has valid names for columns
        for (String key : htblColNameValue.keySet()) {
            if (!t.htblColNameType.keySet().contains(key)) {
                throw new DBAppException("Column does not exist");
            }
        }
        try {
            t.update(strClusteringKeyValue, htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("Error updating table" + e.getMessage());
        }
        serializeTable(t);
    }

    public static void deleteFromTable(String strTableName,
            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            BufferedReader CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();

            // check if strTableName exists
            // check if column exists
            boolean tableExists = false;
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(strTableName)) {
                    tableExists = true;
                }
                line = CSVreader.readLine();
            }
            if (!tableExists) {
                CSVreader.close();
                throw new DBAppException("Table does not exist");
            }
            CSVreader.close();
        } catch (Exception e) {
            // handle the exception as needed
            throw new DBAppException("File error" + e.getMessage());

        }

        Table t = deserializeTable(strTableName);
        // check row has valid names for columns
        for (String key : htblColNameValue.keySet()) {
            if (!t.htblColNameType.keySet().contains(key)) {
                throw new DBAppException("Column does not exist");
            }
        }
        try {
            t.delete(htblColNameValue);
        } catch (IOException e) {
            throw new DBAppException("Error deleting from table" + e.getMessage());
        }
        serializeTable(t);

    }

    // make a method to print all table pages
    public static void getPages(String strTableName) throws DBAppException {
        Table t = deserializeTable(strTableName);
        t.printAllPages();
        serializeTable(t);
    }

    // following method creates an octree depending on the count of column names
    // passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        if (strarrColName.length < 3) {

            throw new DBAppException("Invalid number of columns");
        }
        BufferedReader CSVreader;
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        String indexname = strarrColName[0] + strarrColName[1] + strarrColName[2];
        // write index name to metadata
        // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
        // 4IndexName,5IndexType, 6min, 7max

        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();

            // check if strTableName exists
            boolean tableExists = false;
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(strTableName)) {
                    tableExists = true;
                    htblColNameType.put(parts[1], parts[2]);
                    htblColNameMin.put(parts[1], parts[6]);
                    htblColNameMax.put(parts[1], parts[7]);

                }
                line = CSVreader.readLine();
            }
            if (!tableExists) {
                CSVreader.close();
                throw new DBAppException("Table does not exist");
            }
            CSVreader.close();

        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());

        }

        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();
            Boolean indexalreadyexist = false;
            // check if indexname exists
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[4].equals(indexname)) {
                    indexalreadyexist = true;
                    htblColNameType.put(parts[1], parts[2]);
                    htblColNameMin.put(parts[1], parts[6]);
                    htblColNameMax.put(parts[1], parts[7]);

                }
                line = CSVreader.readLine();
            }
            if (indexalreadyexist) {
                CSVreader.close();
                throw new DBAppException("Index already exists");
            }
            CSVreader.close();

        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());

        }
        // check if columns exist
        for (String col : strarrColName) {
            if (!htblColNameType.containsKey(col)) {
                throw new DBAppException("Column does not exist");
            }
        }
        // check if columns has min and max values and not nulls
        for (String col : strarrColName) {
            if (htblColNameMin.get(col) == null || htblColNameMax.get(col) == null) {
                throw new DBAppException("Column does not have min and max values");
            }
        }
        // write in meta data file
        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            BufferedWriter CSVwriter = new BufferedWriter(new FileWriter("src/main/resources/metadata_temp.csv"));
            String line = CSVreader.readLine();
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(strTableName)) {
                    if (parts[1].equals(strarrColName[0]) || parts[1].equals(strarrColName[1])
                            || parts[1].equals(strarrColName[2])) {
                        parts[4] = indexname;
                        parts[5] = "octree";
                    }
                }
                String newLine = String.join(",", parts); // join the parts with commas
                CSVwriter.write(newLine); // write the new line to the file
                CSVwriter.newLine(); // add a newline character after the line
                line = CSVreader.readLine();
            }
            CSVreader.close();
            CSVwriter.close();

            // Replace the old metadata file with the new one
            File oldFile = new File("src/main/resources/metadata.csv");
            File newFile = new File("src/main/resources/metadata_temp.csv");
            if (oldFile.delete()) {
                newFile.renameTo(oldFile);
            } else {
                throw new DBAppException("Could not update metadata file.");
            }
        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());
        }

        // create index
        Table t = deserializeTable(strTableName);
        Node newindexroot;
        try {
            newindexroot = new Node("-1", strarrColName, 0, indexname);
            t.indices.add(newindexroot);
            // t.indices.get(0).createchildren();
            newindexroot.createchildren();
            t.hasindex = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();

        }

        if (t.pageInfos.size() > 0) {
            // fe pages yaany fe records yaany e3ml index alehom
            t.fillindex(strarrColName, indexname);
        }
        serializeTable(t);

        return;

    }

    public static void printindex(String strTableName) throws DBAppException {
        Table t = deserializeTable(strTableName);
        // getPages(t.tableName);
        t.printindex();
        serializeTable(t);

    }

    public Vector<Row> selectFromTable(SQLTerm[] SqlTerms, String[] strarrOperators)
            throws DBAppException {

        // checks for valid inputs

        // check if table exists using csv file
        BufferedReader CSVreader;
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        String strClusteringKeyColumn = "";

        try {
            CSVreader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = CSVreader.readLine();

            // check if strTableName exists
            boolean tableExists = false;
            while (line != null) {
                String[] parts = line.split(",");
                // 0Table Name, 1Column Name, 2Column Type, 3ClusteringKey,
                // 4IndexName,5IndexType, 6min, 7max
                if (parts[0].equals(SqlTerms[0]._strTableName)) {
                    tableExists = true;
                    if (parts[3].equals("True")) {
                        strClusteringKeyColumn = parts[1];
                    }
                    htblColNameType.put(parts[1], parts[2]);
                    htblColNameMin.put(parts[1], parts[6]);
                    htblColNameMax.put(parts[1], parts[7]);
                }
                line = CSVreader.readLine();
            }
            if (!tableExists) {
                CSVreader.close();
                throw new DBAppException("Table does not exist");
            }
            CSVreader.close();

        } catch (IOException e) {
            throw new DBAppException("File does not exist." + e.getMessage());

        }

        // 2- check if columns exist
        for (SQLTerm term : SqlTerms) {
            if (!htblColNameType.containsKey(term._strColumnName)) {
                throw new DBAppException("Column does not exist");
            }
        }

        if (SqlTerms.length == 0) {
            throw new DBAppException("Invalid SQL term");
        }
        if (strarrOperators.length != SqlTerms.length - 1) {
            throw new DBAppException("Invalid number of operators");
        }
        for (int i = 0; i < strarrOperators.length; i++) {
            if (!strarrOperators[i].equals("AND") && !strarrOperators[i].equals("OR")
                    && !strarrOperators[i].equals("XOR")) {
                throw new DBAppException("Invalid operator");
            }
        }
        for (int l = 0; l < SqlTerms.length; l++) {
            String table = SqlTerms[l]._strTableName;
            String column = SqlTerms[l]._strColumnName;
            String operator = SqlTerms[l]._strOperator;
            Object value = SqlTerms[l]._objValue;

            if (table == null || column == null || operator == null || value == null) {
                throw new DBAppException("Invalid SQL term");

            }
            if (!operator.equals("=") && !operator.equals("!=") && !operator.equals(">")
                    && !operator.equals("<")
                    && !operator.equals(">=") && !operator.equals("<=")) {
                throw new DBAppException("Invalid operator");
            }

        }

        Vector<Vector<Row>> resultVector = new Vector<Vector<Row>>();
        Vector<Row> finalresult = new Vector<Row>();
        int numofnulls = 0;
        Table t = deserializeTable(SqlTerms[0]._strTableName);
        String cluster = t.clusteringKeyColName;
        Boolean singleselect = false;
        if (strarrOperators.length == 0) {
            singleselect = true;
        }
        boolean octway = true;
        for (int i = 0; i < strarrOperators.length; i++) {
            if (!strarrOperators[i].equalsIgnoreCase("AND"))
                octway = false;

        }
        if (SqlTerms.length < 3) {
            octway = false;
        }
        Vector<SQLTerm> SqlTerms2 = new Vector<SQLTerm>();
        for (int i = 0; i < SqlTerms.length; i++) {
            SqlTerms2.add(SqlTerms[i]);
        }
        String[] strarrOperators2 = new String[strarrOperators.length];
        for (int i = 0; i < strarrOperators.length; i++) {
            strarrOperators2[i] = strarrOperators[i];
        }
        int octreesnumber = 0;
        if (octway) {
            for (int i = 0; i < SqlTerms2.size(); i++) {
                Vector<SQLTerm> tmpsqls = new Vector<SQLTerm>();
                Object[] checks = new Object[2];
                if (SqlTerms2.size() >= 3) {
                    SQLTerm[] temparr = new SQLTerm[3];
                    temparr[0] = SqlTerms2.get(0);
                    temparr[1] = SqlTerms2.get(1);
                    temparr[2] = SqlTerms2.get(2);
                    checks = checkoctree(t, temparr);
                    boolean flag = (boolean) checks[0];
                    if (flag && ((SqlTerms2.get(0)._strOperator.equals("="))
                            && (SqlTerms2.get(1)._strOperator.equals("="))
                            && (SqlTerms2.get(2)._strOperator.equals("=")))) {
                        tmpsqls.add(SqlTerms2.get(0));
                        tmpsqls.add(SqlTerms2.get(1));
                        tmpsqls.add(SqlTerms2.get(2));
                        octreesnumber++;
                        // create hashtable<String,Object> for values
                        Hashtable<String, Object> values = new Hashtable<String, Object>();
                        values.put(SqlTerms2.get(0)._strColumnName, SqlTerms2.get(0)._objValue);
                        values.put(SqlTerms2.get(1)._strColumnName, SqlTerms2.get(1)._objValue);
                        values.put(SqlTerms2.get(2)._strColumnName, SqlTerms2.get(2)._objValue);
                        Vector<Row> matchingRows = getrowsoctree(values, (Node) t.indices.get((Integer) checks[1]), t,
                                htblColNameType,
                                htblColNameMin, htblColNameMax, tmpsqls);
                        resultVector.add(matchingRows);
                        SqlTerms2.remove(0);
                        SqlTerms2.remove(0);
                        SqlTerms2.remove(0);
                        i -= 3;

                    } else {
                        octway = false;
                        resultVector.clear();
                        for (int k = 0; k < SqlTerms.length; k++) {
                            resultVector.add(LinearSearch(SqlTerms[k], t));

                        }
                        break;
                    }

                } else if (SqlTerms2.size() != 0 && SqlTerms2.size() < 3) {
                    octway = false;
                    resultVector.clear();
                    for (int k = 0; k < SqlTerms.length; k++) {
                        resultVector.add(LinearSearch(SqlTerms[k], t));

                    }
                    break;
                }
            }
        } else {
            octway = false;
            resultVector.clear();
            for (int k = 0; k < SqlTerms.length; k++) {
                resultVector.add(LinearSearch(SqlTerms[k], t));

            }
        }

        if (octway) {
            String[] strarrOperators3 = new String[octreesnumber - 1];
            for (int i = 0; i < octreesnumber - 1; i++) {
                strarrOperators3[i] = "AND";
            }
            strarrOperators = strarrOperators3;
        }
        if (singleselect) {
            resultVector.add(LinearSearch(SqlTerms[0], t));
        }
        if (resultVector.size() == 1) {
            finalresult = resultVector.get(0);
            return finalresult;
        }

        for (int i = 0; i < strarrOperators.length; i++) {
            Vector<Row> v1 = new Vector<Row>();
            Vector<Row> v2 = new Vector<Row>();
            if (i == 0) {
                v1 = resultVector.get(0);
                v2 = resultVector.get(1);
                resultVector.remove(0);
                resultVector.remove(0);
            } else {
                v1 = finalresult;
                v2 = resultVector.get(0);
                resultVector.remove(0);
            }
            Vector<Row> temp = new Vector<Row>();
            if (strarrOperators[i] == null) {
                continue;

            }
            if (strarrOperators[i].equalsIgnoreCase("AND")) {

                for (int j = 0; j < v1.size(); j++) {
                    for (int k = 0; k < v2.size(); k++) {
                        if (v1.get(j).values.get(SqlTerms[i]._strColumnName)
                                .equals(v2.get(k).values.get(SqlTerms[i]._strColumnName))) { // "dah el hato el copilot"
                            temp.add(v1.get(j));

                        }
                    }
                }
                finalresult = temp;
            } else if (strarrOperators[i].equalsIgnoreCase("OR")) {

                Hashtable<Object, Object> H = new Hashtable<Object, Object>();
                for (int j = 0; j < v1.size(); j++) {
                    H.put(v1.get(j).values.get(SqlTerms[i]._strColumnName), v1.get(j));// clster
                    temp.add(v1.get(j));
                }
                for (int j = 0; j < v2.size(); j++) {
                    if (!H.containsKey(v2.get(j).values.get(SqlTerms[i]._strColumnName))) { // clustering
                        temp.add(v2.get(j));
                    }
                }

                finalresult = temp;

            } else if (strarrOperators[i].equalsIgnoreCase("XOR")) {
                Hashtable<Object, Object> H1 = new Hashtable<Object, Object>();
                Hashtable<Object, Object> H2 = new Hashtable<Object, Object>();
                for (int k = 0; k < v1.size(); k++) {
                    H1.put(v1.get(k).values.get(cluster), v1);
                }
                for (int k = 0; k < v2.size(); k++) {
                    H2.put(v2.get(k).values.get(cluster), v2);
                }
                for (int k = 0; k < v1.size(); k++) {
                    if (!H2.contains(v1.get(k).values.get(cluster))) {
                        temp.add(v1.get(k));
                    }
                }
                for (int k = 0; k < v2.size(); k++) {
                    if (!H1.contains(v2.get(k).values.get(cluster))) {
                        temp.add(v2.get(k));
                    }
                }
                finalresult = temp;
            }

        }
        if (singleselect) {
            finalresult = resultVector.get(0);

        }
        return finalresult;

    }

    public static Vector<Row> getrowsoctree(Hashtable<String, Object> values, Node idx, Table t,
            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax, Vector<SQLTerm> indexforsql) {
        Vector<Row> result = new Vector<Row>();

        // use octree
        // get index

        Object[] noneed = new Object[0];
        Vector<Integer> returnpagenumber = new Vector<Integer>();
        Vector<Integer> pages = new Vector<Integer>();
        try {
            pages = idx.findRows(values, htblColNameType, htblColNameMin, htblColNameMax,
                    0, noneed, "", returnpagenumber);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
        // deserialize pages and add to resultVector
        for (int i = 0; i < pages.size(); i++) {
            Vector<Row> page_data = Table.deserializePage(t.tableName + '_' + pages.get(i));
            for (int j = 0; j < page_data.size(); j++) {
                Row currow = page_data.get(j);

                boolean flag = true;
                for (int k = 0; k < 3; k++) {
                    String column = indexforsql.get(k)._strColumnName;
                    String operator = indexforsql.get(k)._strOperator;
                    Object value = indexforsql.get(k)._objValue;
                    if (value instanceof String) {
                        if (operator.equals("=")) {
                            if (!currow.values.get(column).equals(value)) {
                                flag = false;
                                break;
                            }
                        }
                    } else if (value instanceof Integer) {
                        if (operator.equals("=")) {
                            if (((Integer) currow.values.get(column)).intValue() != ((Integer) value).intValue()) {
                                flag = false;
                                break;
                            }
                        }

                    } else if (value instanceof Double) {
                        if (operator.equals("=")) {
                            if (((Double) currow.values.get(column)).doubleValue() != ((Double) value).doubleValue()) {
                                flag = false;
                                break;
                            }
                        }

                    } else if (value instanceof Date) {
                        if (operator.equals("=")) {
                            if (((Date) currow.values.get(column)).compareTo((Date) value) != 0) {
                                flag = false;
                                break;
                            }
                        }

                    }
                    if (flag) {
                        result.add(currow);
                        flag = false;
                    }
                }
                Table.serializePage(page_data, t.tableName + '_' + pages.get(i));
            }

        }
        return result;
    }

    public Vector<Row> LinearSearch(SQLTerm SqlTerms, Table t) throws DBAppException {
        Vector<Row> result = new Vector<Row>();
        String operator = SqlTerms._strOperator;
        Object value = SqlTerms._objValue;
        String column = SqlTerms._strColumnName;

        for (int i = 0; i < t.pageInfos.size(); i++) {
            Vector<Row> page_data = Table.deserializePage(t.tableName + '_' + i);

            for (int j = 0; j < page_data.size(); j++) {

                Row row = page_data.get(j);
                if (operator == "=") {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (columnValue.equals(value)) {
                            result.add(row);
                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {

                        if (columnValue.equals(value)) {
                            result.add(row);
                        }
                    } else if (value instanceof String && columnValue instanceof String) {

                        if (((String) columnValue).compareTo((String) value) == 0) {

                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) == 0) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                } else if (operator == "!=") {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (!columnValue.equals(value)) {
                            result.add(row);
                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {
                        if (!columnValue.equals(value)) {
                            result.add(row);
                        }
                    } else if (value instanceof String && columnValue instanceof String) {
                        if (((String) columnValue).compareTo((String) value) != 0) {
                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) != 0) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                } else if (operator.equals(">")) {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (((Integer) columnValue).intValue() > ((Integer) value).intValue()) {
                            result.add(row);
                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {
                        if (((Double) columnValue).doubleValue() > ((Double) value).doubleValue()) {
                            result.add(row);
                        }
                    } else if (value instanceof String && columnValue instanceof String) {
                        if (((String) columnValue).compareTo((String) value) > 0) {
                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) > 0) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                } else if (operator.equals("<")) {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (((Integer) columnValue).intValue() < ((Integer) value).intValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {
                        if (((Double) columnValue).doubleValue() < ((Double) value).doubleValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof String && columnValue instanceof String) {
                        if (((String) columnValue).compareTo((String) value) < 0) {
                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) < 0) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                } else if (operator.equals(">=")) {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (((Integer) columnValue).intValue() >= ((Integer) value).intValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {
                        if (((Double) columnValue).doubleValue() >= ((Double) value).doubleValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof String && columnValue instanceof String) {
                        if (((String) columnValue).compareTo((String) value) > 0 ||
                                ((String) columnValue).equals((String) value)) {
                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) > 0 ||
                                ((Date) columnValue).equals((Date) value)) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                } else if (operator == "<=") {
                    Object columnValue = row.values.get(column);
                    if (value instanceof Integer && columnValue instanceof Integer) {
                        if (((Integer) columnValue).intValue() <= ((Integer) value).intValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof Double && columnValue instanceof Double) {
                        if (((Double) columnValue).doubleValue() <= ((Double) value).doubleValue()) {
                            result.add(row);

                        }
                    } else if (value instanceof String && columnValue instanceof String) {
                        if (((String) columnValue).compareTo((String) value) < 0 ||
                                ((String) columnValue).equals((String) value)) {
                            result.add(row);

                        }
                    } else if (value instanceof Date && columnValue instanceof Date) {
                        if (((Date) columnValue).compareTo((Date) value) < 0 ||
                                ((Date) columnValue).equals((Date) value)) {
                            result.add(row);
                        }
                    } else {
                        throw new DBAppException("Invalid value type for comparison");
                    }
                }
            }

        }
        return result;
    }

    public static Object[] checkoctree(Table t, SQLTerm[] SqlTerms)
            throws DBAppException {
        Object[] result = new Object[2];
        Boolean octway = false;
        int indexnum = -1;
        result[0] = octway;
        result[1] = indexnum;

        for (int k = 0; k < t.indices.size(); k++) {
            // check if index contains all columns using column names
            Node idx = t.indices.get(k);
            boolean flag = true;
            String[] colnames = idx.columnnames;
            for (int j = 0; j < 3; j++) {
                if (!(colnames[j].equals(SqlTerms[0]._strColumnName)
                        || colnames[j].equals(SqlTerms[1]._strColumnName)
                        || colnames[j].equals(SqlTerms[2]._strColumnName))) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                octway = true;
                indexnum = k;
                result[0] = octway;
                result[1] = indexnum;
                return result;
            }

        }
        return result;
    }

    private static void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("C:\\Users\\zeyad\\Desktop\\DB2\\src\\main\\courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

 private static void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("C:\\Users\\zeyad\\Desktop\\DB2\\src\\main\\students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
 private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("C:\\Users\\zeyad\\Desktop\\DB2\\src\\main\\transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
 private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("C:\\Users\\zeyad\\Desktop\\DB2\\src\\main\\pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
 private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }
  public static void main(String[] args) throws Exception {
      DBApp db = new DBApp();
      db.init();
    //   createCoursesTable(db);
    //   createPCsTable(db);
    //   createTranscriptsTable(db);
    //   createStudentTable(db);
    //   insertPCsRecords(db,500);
    //   insertTranscriptsRecords(db,500);
    //   insertStudentRecords(db,500);
    //   insertCoursesRecords(db,500);

    // Hashtable <String, Object> hash = new Hashtable<>();
    // hash.put("gpa", new Double(3.12));
    // // // hash.put("course_id", new String("1207"));
    // // // hash.put("date_added", new Date(1942-1900,10-1,27));


    // db.deleteFromTable("students",  hash);
    // db.getPages("students");

    // SQLTerm sql = new SQLTerm("courses", "course_id", "=", new String("0950"));
    // SQLTerm sql2 = new SQLTerm("courses", "course_name", "=", new String("FjGZmL"));
    // SQLTerm sql3 = new SQLTerm("courses", "date_added", "=", new Date(2008 - 1900, 6-1, 31));
    // // SQLTerm sql4 = new SQLTerm("Student", "id", "=", 0);
    // // SQLTerm sql5 = new SQLTerm("Student", "name", "=", "gizawy");
    // // SQLTerm sql6 = new SQLTerm("Student", "Date of Birth", "=", new Date(2015 - 1900, 9 - 1, 17));
    // SQLTerm[] arrSQLTerms = new SQLTerm[3];
    // arrSQLTerms[0] = sql;
    // arrSQLTerms[1] = sql2;
    // arrSQLTerms[2] = sql3;
    // // arrSQLTerms[3] = sql4;
    // // arrSQLTerms[4] = sql5;
    // // arrSQLTerms[5] = sql6;
    // String[] strarrOperators = new String[2];
    // strarrOperators[0] = "AND";
    // strarrOperators[1] = "AND";
    // // strarrOperators[2] = "AND";
    // // strarrOperators[3] = "AND";
    // // strarrOperators[4] = "AND";
    // Vector<Row> x = db.selectFromTable(arrSQLTerms, strarrOperators);
    // System.out.println("x size: " + x.size());
    // for (int i = 0; i < x.size(); i++) {

    //     System.out.println(x.get(i).values);
    // }

    String[] strarrColName= {"course_id","hours","date_added"};
		try {
			db.createIndex("courses", strarrColName);
		} catch ( DBAppException e) {
            System.out.println("hi");
			// TODO Auto-generated catch block
			e.printStackTrace();
            
		}
    // db.getPages("students");
      db.printindex("courses");
      
  }

}
