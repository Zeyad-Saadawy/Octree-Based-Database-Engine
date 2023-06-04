import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Node implements java.io.Serializable {
	String number;
	Vector<Node> children;
	Vector<Address> refrences;
	String[] columnnames;
	int maxcapacity;
	Node parent;
	int level;
	String indexname;
	Node nextsiblingNode;

	public Node(String number, String strarrColName[], int level, String indexname) throws FileNotFoundException {
		this.number = number;
		this.children = null;
		this.refrences = new Vector<Address>();
		this.columnnames = strarrColName;
		this.parent = null;
		this.level = level;
		try {
			this.maxcapacity = readMaximumsizeCountinNode();
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.indexname = indexname;
		this.nextsiblingNode = null;

	}

	public void createchildren() throws FileNotFoundException {
		this.children = new Vector<Node>();
		Node prevChild = null;
		for (int i = 0; i < 8; i++) {
			String binaryString = Integer.toBinaryString(i);
			while (binaryString.length() < 3) {
				binaryString = "0" + binaryString;
			}
			int newlevel = this.level + 1;
			Node childNode = new Node(binaryString, this.columnnames, newlevel, this.indexname);
			childNode.parent = this;
			this.children.add(childNode);
			if (prevChild != null) {
				prevChild.nextsiblingNode = childNode;
			}
			prevChild = childNode;
		}
	}

	public static int readMaximumsizeCountinNode() throws FileNotFoundException, DBAppException {
		BufferedReader reader;

		try {
			reader = new BufferedReader(new FileReader("src/main/resources/DBApp.config"));
			String line = reader.readLine();

			while (line != null) {
				String[] parts = line.split("=");
				if (parts[0].equals("MaximumEntriesinOctreeNode ")) {
					reader.close();
					return Integer.parseInt(parts[1].replaceAll(" ", ""));
				}

				line = reader.readLine();
			}
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("File does not exist.");
			throw new DBAppException("File does not exist.");

		}
		return 0;

	}

	public void insertintoIndex(Row row, Address address, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			int levelargument, Object[] midValarrgument, String resultarrgument) throws DBAppException {
		// get the middle value for 3 columns
		Object col1min = null;
		Object col1max = null;
		Object col2min = null;
		Object col2max = null;
		Object col3min = null;
		Object col3max = null;
		for (int i = 0; i < columnnames.length; i++) {
			if (htblColNameType.get(columnnames[i]).equals("java.lang.Integer")) {
				if (i == 0) {
					col1min = Integer.parseInt(htblColNameMin.get(columnnames[0]));
					col1max = Integer.parseInt(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Integer.parseInt(htblColNameMin.get(columnnames[1]));
					col2max = Integer.parseInt(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Integer.parseInt(htblColNameMin.get(columnnames[2]));
					col3max = Integer.parseInt(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.String")) {
				if (i == 0) {
					col1min = htblColNameMin.get(columnnames[0]);
					col1max = htblColNameMax.get(columnnames[0]);
				} else if (i == 1) {
					col2min = htblColNameMin.get(columnnames[1]);
					col2max = htblColNameMax.get(columnnames[1]);
				} else if (i == 2) {
					col3min = htblColNameMin.get(columnnames[2]);
					col3max = htblColNameMax.get(columnnames[2]);
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.Double")) {
				if (i == 0) {
					col1min = Double.parseDouble(htblColNameMin.get(columnnames[0]));
					col1max = Double.parseDouble(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Double.parseDouble(htblColNameMin.get(columnnames[1]));
					col2max = Double.parseDouble(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Double.parseDouble(htblColNameMin.get(columnnames[2]));
					col3max = Double.parseDouble(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				if (i == 0) {
					try {
						col1min = formatter.parse(htblColNameMin.get(columnnames[0]));
						col1max = formatter.parse(htblColNameMax.get(columnnames[0]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 1) {
					try {
						col2min = formatter.parse(htblColNameMin.get(columnnames[1]));
						col2max = formatter.parse(htblColNameMax.get(columnnames[1]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 2) {
					try {
						col3min = formatter.parse(htblColNameMin.get(columnnames[2]));
						col3max = formatter.parse(htblColNameMax.get(columnnames[2]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}

		}
		// System.out.println("min: " + col1min + " max: " + col1max);
		Object[] midVal = null;
		if (levelargument == 0) {
			midVal = computeMidVal3Col(col1min, col1max,
					col2min, col2max,
					col3min, col3max);
		} else {
			midVal = computeMidVal3Col2(col1min, col1max, midValarrgument[0],
					col2min, col2max, midValarrgument[1],
					col3min, col3max, midValarrgument[2], resultarrgument);
		}

		// check if the node isleaf
		if (this.children == null) {
			// if the node is leaf
			// check if the node is full
			if (this.refrences.size() < this.maxcapacity) {
				// if the node is not full
				// insert the row in the node
				this.refrences.add(address);
				return;
			} else {

				// if the node is full
				// create 8 children
				try {
					this.createchildren();
				} catch (FileNotFoundException e) {
					System.out.println("error in creating children " + e.getMessage());
					throw new DBAppException("error in creating children " + e.getMessage());
				}

				// insert the old rows in the children
				for (int i = 0; i < this.refrences.size(); i++) {
					// get the index of the child that the row should be inserted in
					Row tempRow = new Row(this.refrences.get(i).actualvalues);
					String index = getnodenum(midVal, tempRow, columnnames);
					// insert the row in the child
					int nodenum = Integer.parseInt(index, 2);
					Address tmpaddress = this.refrences.get(i);
					Node tmpNode = this.children.get(nodenum);
					this.children.get(nodenum).insertintoIndex(tempRow, tmpaddress, htblColNameType,
							htblColNameMin, htblColNameMax, tmpNode.level, midVal, index);
				}
				// clear the old rows
				this.refrences.clear();
				// insert the new row in the children
				// get the index of the child that the row should be inserted in
				String index = getnodenum(midVal, row, columnnames);
				// insert the row in the child
				int nodenum = Integer.parseInt(index, 2);
				Node newchNode = this.children.get(nodenum);
				this.children.get(nodenum).insertintoIndex(row, address, htblColNameType, htblColNameMin,
						htblColNameMax, newchNode.level, midVal, index);
			}

		} else {
			// if the node is not leaf
			// get the index of the child that the row should be inserted in
			String index = getnodenum(midVal, row, columnnames);
			// insert the row in the child
			int nodenum = Integer.parseInt(index, 2);
			Node newchNode = this.children.get(nodenum);
			this.children.get(nodenum).insertintoIndex(row, address, htblColNameType, htblColNameMin, htblColNameMax,
					newchNode.level, midVal, index);
		}

	}

	public static String getnodenum(Object[] midVal, Row row, String[] columnnames) {
		String result = "";
		for (int i = 0; i < columnnames.length; i++) {
			if (midVal[i] instanceof Integer) {
				// get row value
				int rowval = (int) row.values.get(columnnames[i]);
				int midVal1 = (int) midVal[i];
				// check if midval1 < row value of this column if yes concat 0 to result else
				// concat 1
				if (rowval < midVal1) {
					result = result + "0";
				} else {
					result = result + "1";
				}
			} else if (midVal[i] instanceof String) {
				// get row value
				String rowval = (String) row.values.get(columnnames[i]);
				String midVal1 = (String) midVal[i];
				// check if midval1 < row value of this column if yes concat 0 to result else
				// concat 1
				if (rowval.compareTo(midVal1) < 0) {
					result = result + "0";
				} else {
					result = result + "1";
				}
			} else if (midVal[i] instanceof Double) {
				// get row value
				double rowval = (double) row.values.get(columnnames[i]);
				double midVal1 = (double) midVal[i];
				// check if midval1 < row value of this column if yes concat 0 to result else
				// concat 1
				if (rowval < midVal1) {
					result = result + "0";
				} else {
					result = result + "1";
				}
			} else if (midVal[i] instanceof Date) {
				// get row value
				Date rowval = (Date) row.values.get(columnnames[i]);
				Date midVal1 = (Date) midVal[i];
				// check if midval1 < row value of this column if yes concat 0 to result else
				// concat 1
				if (rowval.compareTo(midVal1) < 0) {
					result = result + "0";
				} else {
					result = result + "1";
				}
			}

		}
		return result;
	}

	public static Object computeMidVal(Object minValue, Object maxValue) throws DBAppException {
		// Check that the input values are not null

		if (minValue == null || maxValue == null) {
			throw new IllegalArgumentException("Invalid input values. Cannot be null.");
		}

		// Check the type of the input values and compute the midpoint value based on
		// the type
		if (minValue instanceof String && maxValue instanceof String) {
			// Cast the input values to strings
			String minStr = (String) minValue;
			String maxStr = (String) maxValue;

			// Get the minimum length between the two strings
			int minLength = Math.min(minStr.length(), maxStr.length());

			// Find the first position where the two strings differ
			int i = 0;
			while (i < minLength && minStr.charAt(i) == maxStr.charAt(i)) {
				i++;
			}

			// If the two strings are equal up to the end of the shorter string, return the
			// shorter string
			if (i == minLength) {
				if (minStr.length() == maxStr.length()) {
					return (Object) minStr.substring(0, i);
				} else if (minStr.length() < maxStr.length()) {
					return (Object) minStr;
				} else {
					return (Object) maxStr;
				}
			}

			// Compute the midpoint value for the first position where the two strings
			// differ
			int intValue1 = (int) maxStr.charAt(i);
			int intValue2 = (int) minStr.charAt(i);
			int midpointIntValue = (intValue2 + intValue1);
			if (midpointIntValue % 2 == 1) {
				midpointIntValue++;
			}
			midpointIntValue /= 2;
			char midpointChar = (char) midpointIntValue;
			String midpointStr = minStr.substring(0, i) + midpointChar;
			return (Object) midpointStr;
		} else if (minValue instanceof Integer && maxValue instanceof Integer) {
			// Cast the input values to integers and compute the midpoint value
			int minInt = (int) minValue;
			int maxInt = (int) maxValue;
			int midpointInt = (minInt + maxInt);
			if (midpointInt % 2 == 1) {
				midpointInt++;
			}
			midpointInt /= 2;
			return (Object) midpointInt;
		} else if (minValue instanceof Double && maxValue instanceof Double) {
			// Cast the input values to doubles and compute the midpoint value
			double minDouble = (double) minValue;
			double maxDouble = (double) maxValue;
			double midpointDouble = (minDouble + maxDouble);
			if (midpointDouble % 2 == 1) {
				midpointDouble++;
			}
			midpointDouble /= 2;
			return (Object) midpointDouble;
		} else if (minValue instanceof Date && maxValue instanceof Date) {
			try {
				Date minDate = (Date) minValue;
				Date maxDate = (Date) maxValue;
				return getMidDate(minDate, maxDate);
			} catch (Exception e) {
				throw new DBAppException("Invalid date format: " + e.getMessage());
			}
		} else {
			throw new DBAppException("Invalid data type");
		}
	}

	public static Object[] computeMidVal3Col(Object minValue1, Object maxValue1, Object minValue2, Object maxValue2,
			Object minValue3, Object maxValue3) throws DBAppException {
		Object[] midVal = new Object[3];
		midVal[0] = computeMidVal(minValue1, maxValue1);
		midVal[1] = computeMidVal(minValue2, maxValue2);
		midVal[2] = computeMidVal(minValue3, maxValue3);
		return midVal;
	}

	public static Object[] computeMidVal3Col2(Object minValue1, Object maxValue1, Object mid1, Object minValue2,
			Object maxValue2, Object mid2, Object minValue3, Object maxValue3, Object mid3, String result)
			throws DBAppException {

		Object[] midVal = new Object[3];
		char c1 = result.charAt(0);
		char c2 = result.charAt(1);
		char c3 = result.charAt(2);
		midVal[0] = computeMidVal2(minValue1, maxValue1, mid1, c1);
		midVal[1] = computeMidVal2(minValue2, maxValue2, mid2, c2);
		midVal[2] = computeMidVal2(minValue3, maxValue3, mid3, c3);
		return midVal;
	}

	private static Object getMidDate(Date date1, Date date2) {
		LocalDate localDate1 = date1.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate localDate2 = date2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

		Period period = Period.between(localDate1, localDate2);
		LocalDate middleLocalDate = localDate1.plus(dividePeriodByTwo(period));

		Instant instant = middleLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
		return Date.from(instant);
	}

	public static Period dividePeriodByTwo(Period period) {
		int years = period.getYears() / 2;
		int months = period.getMonths() / 2;
		int days = period.getDays() / 2;
		return Period.of(years, months, days);
	}

	public static void serializePage(Vector<Row> data, String pageName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + pageName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(data);
			out.reset();
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Vector<Row> deserializePage(String pageName) {
		Vector<Row> data = null;
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + pageName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			data = (Vector<Row>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			System.out.println("Vector<Row> class not found");
			c.printStackTrace();
			return null;
		}
		return data;
	}

	public static Object computeMidVal2(Object minValue, Object maxValue, Object middle, char result)
			throws DBAppException {
		// Check that the input values are not null
		if (minValue == null || maxValue == null || middle == null) {
			throw new IllegalArgumentException("Invalid input values. Cannot be null.");
		}

		// Check the type of the input values and compute the midpoint value based on
		// the type
		if (minValue instanceof String && maxValue instanceof String && middle instanceof String) {
			// Cast the input values to strings
			String minStr = null;
			String maxStr = null;
			if (result == '0') {
				minStr = (String) minValue;
				maxStr = (String) middle;
			} else if (result == '1') {
				minStr = (String) middle;
				maxStr = (String) maxValue;
			} else {
				throw new DBAppException("Invalid result");
			}

			// Get the minimum length between the two strings
			int minLength = Math.min(minStr.length(), maxStr.length());

			// Find the first position where the two strings differ
			int i = 0;
			while (i < minLength && minStr.charAt(i) == maxStr.charAt(i)) {
				i++;
			}

			// If the two strings are equal up to the end of the shorter string, return the
			// shorter string
			if (i == minLength) {
				if (minStr.length() == maxStr.length()) {
					return (Object) minStr.substring(0, i);
				} else if (minStr.length() < maxStr.length()) {
					return (Object) minStr;
				} else {
					return (Object) maxStr;
				}
			}

			// Compute the midpoint value for the first position where the two strings
			// differ
			int intValue1 = (int) maxStr.charAt(i);
			int intValue2 = (int) minStr.charAt(i);
			int midpointIntValue = (intValue2 + intValue1);
			if (midpointIntValue % 2 == 1) {
				midpointIntValue++;
			}
			midpointIntValue /= 2;
			char midpointChar = (char) midpointIntValue;
			String midpointStr = minStr.substring(0, i) + midpointChar;
			return (Object) midpointStr;
		} else if (minValue instanceof Integer && maxValue instanceof Integer && middle instanceof Integer) {
			// Cast the input values to integers and compute the midpoint value

			int minInt = (int) minValue;
			int maxInt = (int) maxValue;
			if (result == '0') {
				minInt = (int) minValue;
				maxInt = (int) middle;
			} else if (result == '1') {
				minInt = (int) middle;
				maxInt = (int) maxValue;
			}
			int midpointInt = (minInt + maxInt);
			if (midpointInt % 2 == 1) {
				midpointInt++;
			}
			midpointInt /= 2;
			return (Object) midpointInt;
		} else if (minValue instanceof Double && maxValue instanceof Double && middle instanceof Double) {
			// Cast the input values to doubles and compute the midpoint value
			double minDouble = (double) minValue;
			double maxDouble = (double) maxValue;
			if (result == '0') {
				minDouble = (double) minValue;
				maxDouble = (double) middle;
			} else if (result == '1') {
				minDouble = (double) middle;
				maxDouble = (double) maxValue;
			}
			double midpointDouble = (minDouble + maxDouble);
			if (midpointDouble % 2 == 1) {
				midpointDouble++;
			}
			midpointDouble /= 2;
			return (Object) midpointDouble;
		} else if (minValue instanceof Date && maxValue instanceof Date && middle instanceof Date) {
			try {
				Date minDate = (Date) minValue;
				Date maxDate = (Date) maxValue;
				if (result == '0') {
					minDate = (Date) minValue;
					maxDate = (Date) middle;
				} else if (result == '1') {
					minDate = (Date) middle;
					maxDate = (Date) maxValue;
				}
				return getMidDate(minDate, maxDate);
			} catch (Exception e) {
				throw new DBAppException("Invalid date format: " + e.getMessage());
			}
		} else {
			throw new DBAppException("Invalid data type");
		}
	}

	public Vector<Integer> deletefromIndex(Row row, Hashtable<String, Object> actualvalues,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			int levelargument, Object[] midValarrgument, String resultarrgument, Vector<Integer> returnpagenumber)
			throws DBAppException {
		// get the middle value for 3 columns
		Object col1min = null;
		Object col1max = null;
		Object col2min = null;
		Object col2max = null;
		Object col3min = null;
		Object col3max = null;

		for (int i = 0; i < columnnames.length; i++) {
			if (htblColNameType.get(columnnames[i]).equals("java.lang.Integer")) {
				if (i == 0) {
					col1min = Integer.parseInt(htblColNameMin.get(columnnames[0]));
					col1max = Integer.parseInt(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Integer.parseInt(htblColNameMin.get(columnnames[1]));
					col2max = Integer.parseInt(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Integer.parseInt(htblColNameMin.get(columnnames[2]));
					col3max = Integer.parseInt(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.String")) {
				if (i == 0) {
					col1min = htblColNameMin.get(columnnames[0]);
					col1max = htblColNameMax.get(columnnames[0]);
				} else if (i == 1) {
					col2min = htblColNameMin.get(columnnames[1]);
					col2max = htblColNameMax.get(columnnames[1]);
				} else if (i == 2) {
					col3min = htblColNameMin.get(columnnames[2]);
					col3max = htblColNameMax.get(columnnames[2]);
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.Double")) {
				if (i == 0) {
					col1min = Double.parseDouble(htblColNameMin.get(columnnames[0]));
					col1max = Double.parseDouble(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Double.parseDouble(htblColNameMin.get(columnnames[1]));
					col2max = Double.parseDouble(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Double.parseDouble(htblColNameMin.get(columnnames[2]));
					col3max = Double.parseDouble(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				if (i == 0) {
					try {
						col1min = formatter.parse(htblColNameMin.get(columnnames[0]));
						col1max = formatter.parse(htblColNameMax.get(columnnames[0]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 1) {
					try {
						col2min = formatter.parse(htblColNameMin.get(columnnames[1]));
						col2max = formatter.parse(htblColNameMax.get(columnnames[1]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 2) {
					try {
						col3min = formatter.parse(htblColNameMin.get(columnnames[2]));
						col3max = formatter.parse(htblColNameMax.get(columnnames[2]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}

		}
		Object[] midVal = null;
		if (levelargument == 0) {
			midVal = computeMidVal3Col(col1min, col1max,
					col2min, col2max,
					col3min, col3max);
		} else {
			midVal = computeMidVal3Col2(col1min, col1max, midValarrgument[0],
					col2min, col2max, midValarrgument[1],
					col3min, col3max, midValarrgument[2], resultarrgument);
		}

		// check if the node isleaf
		if (this.children == null) {
			// check for the correct address and get the page number add it to
			// returnpagenumber
			for (int i = 0; i < this.refrences.size(); i++) {
				Hashtable<String, Object> curraddress = this.refrences.get(i).actualvalues;
				boolean flag = true;
				for (int j = 0; j < columnnames.length; j++) {
					if (curraddress.containsKey(columnnames[j])) {
						if (!curraddress.get(columnnames[j]).equals(actualvalues.get(columnnames[j]))) {
							flag = false;
							break;
						}
					} else {
						flag = false;
						break;
					}
				}
				if (flag) {
					returnpagenumber.add(this.refrences.get(i).pagenum);
					this.refrences.remove(i);
					i--;
				}
			}
			
			// check if the leaf if empty
			if (this.refrences.size() == 0) {
				// check if the siblings are empty and has no children
				deleteEmptySiblings();
			}
			return returnpagenumber;

		} else {
			// if the node is not leaf
			// get the index of the child that the row should be inserted in
			String index = getnodenum(midVal, row, columnnames);
			// insert the row in the child
			int nodenum = Integer.parseInt(index, 2);
			Node newchNode = this.children.get(nodenum);
			returnpagenumber = this.children.get(nodenum).deletefromIndex(row, actualvalues, htblColNameType,
					htblColNameMin, htblColNameMax, newchNode.level, midVal, index, returnpagenumber);
			return returnpagenumber;

		}
	}

	public void print() {

		System.out.println("Node " + number + ":");
		System.out.println("\tLevel: " + level);
		System.out.println("\tParent Node: " + (parent != null ? parent.number : "null"));
		System.out.println("\tNext Sibling Node: " + (nextsiblingNode != null ? nextsiblingNode.number : "null"));
		System.out.println("\tChildren Vector Size: " + (children != null ? children.size() : 0));
		System.out.println("\tRefrences Vector Size: " + refrences.size());
		System.out.println("\tRefrences:");
		for (Address address : refrences) {
			System.out.println("\t\tPage Number: " + address.pagenum);
			System.out.println("\t\tCluster key: " + address.clusteringkey);
			System.out.println("\t\tTable Name: " + address.tablename);
			// print actual values
			System.out.println("\t\tActual Values:");
			for (String key : address.actualvalues.keySet()) {
				System.out.println("\t\t\t" + key + ": " + address.actualvalues.get(key));
			}
		}
		if (children != null) {
			for (Node child : children) {
				child.print();
			}
		}

	}

	public void deleteEmptySiblings() {
		if (this.parent != null) {
			Node parent = this.parent;
			if (parent.number.equals("-1")) {
				return;
			} else {
				boolean flag = true;
				for (int i = 0; i < parent.children.size(); i++) {
					if (parent.children.get(i).refrences.size() != 0 || parent.children.get(i).children != null) {
						flag = false;
						break;
					}
				}
				if (flag) {
					parent.children = null;
					parent.refrences.clear();
					parent.deleteEmptySiblings();
				}
			}
		} else {
			return;
		}
	}

	public void updatepagenum(Row row, Hashtable<String, Object> actualvalues,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			int levelargument, Object[] midValarrgument, String resultarrgument,
			int newpagenum, int oldpagenum)
			throws DBAppException {
		// get the middle value for 3 columns
		Object col1min = null;
		Object col1max = null;
		Object col2min = null;
		Object col2max = null;
		Object col3min = null;
		Object col3max = null;

		for (int i = 0; i < columnnames.length; i++) {
			if (htblColNameType.get(columnnames[i]).equals("java.lang.Integer")) {
				if (i == 0) {
					col1min = Integer.parseInt(htblColNameMin.get(columnnames[0]));
					col1max = Integer.parseInt(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Integer.parseInt(htblColNameMin.get(columnnames[1]));
					col2max = Integer.parseInt(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Integer.parseInt(htblColNameMin.get(columnnames[2]));
					col3max = Integer.parseInt(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.String")) {
				if (i == 0) {
					col1min = htblColNameMin.get(columnnames[0]);
					col1max = htblColNameMax.get(columnnames[0]);
				} else if (i == 1) {
					col2min = htblColNameMin.get(columnnames[1]);
					col2max = htblColNameMax.get(columnnames[1]);
				} else if (i == 2) {
					col3min = htblColNameMin.get(columnnames[2]);
					col3max = htblColNameMax.get(columnnames[2]);
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.Double")) {
				if (i == 0) {
					col1min = Double.parseDouble(htblColNameMin.get(columnnames[0]));
					col1max = Double.parseDouble(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Double.parseDouble(htblColNameMin.get(columnnames[1]));
					col2max = Double.parseDouble(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Double.parseDouble(htblColNameMin.get(columnnames[2]));
					col3max = Double.parseDouble(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				if (i == 0) {
					try {
						col1min = formatter.parse(htblColNameMin.get(columnnames[0]));
						col1max = formatter.parse(htblColNameMax.get(columnnames[0]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 1) {
					try {
						col2min = formatter.parse(htblColNameMin.get(columnnames[1]));
						col2max = formatter.parse(htblColNameMax.get(columnnames[1]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 2) {
					try {
						col3min = formatter.parse(htblColNameMin.get(columnnames[2]));
						col3max = formatter.parse(htblColNameMax.get(columnnames[2]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}

		}
		Object[] midVal = null;
		if (levelargument == 0) {
			midVal = computeMidVal3Col(col1min, col1max,
					col2min, col2max,
					col3min, col3max);
		} else {
			midVal = computeMidVal3Col2(col1min, col1max, midValarrgument[0],
					col2min, col2max, midValarrgument[1],
					col3min, col3max, midValarrgument[2], resultarrgument);
		}

		// check if the node isleaf
		if (this.children == null) {
			// check for the correct address and change the pagenum
			for (int i = 0; i < this.refrences.size(); i++) {
				Hashtable<String, Object> curraddress = this.refrences.get(i).actualvalues;
				boolean flag = true;
				for (int j = 0; j < columnnames.length; j++) {
					if (curraddress.containsKey(columnnames[j])) {
						if (!curraddress.get(columnnames[j]).equals(actualvalues.get(columnnames[j]))) {
							flag = false;
							break;
						}
					} else {
						flag = false;
						break;
					}
				}
				if (flag && this.refrences.get(i).pagenum == oldpagenum) {
					this.refrences.get(i).pagenum = newpagenum;
					return;
				}
			}

			return;

		} else {
			// if the node is not leaf
			// get the index of the child that the row should be inserted in
			String index = getnodenum(midVal, row, columnnames);
			// insert the row in the child
			int nodenum = Integer.parseInt(index, 2);
			Node newchNode = this.children.get(nodenum);
			this.children.get(nodenum).updatepagenum(row, actualvalues, htblColNameType, htblColNameMin, htblColNameMax,
					levelargument, midValarrgument, resultarrgument, oldpagenum, newpagenum);
			return;

		}
	}

	public void deleteforupdate(Row row, Hashtable<String, Object> actualvalues,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			int levelargument, Object[] midValarrgument, String resultarrgument,
			int page_number)
			throws DBAppException {
		// get the middle value for 3 columns
		Object col1min = null;
		Object col1max = null;
		Object col2min = null;
		Object col2max = null;
		Object col3min = null;
		Object col3max = null;

		for (int i = 0; i < columnnames.length; i++) {
			if (htblColNameType.get(columnnames[i]).equals("java.lang.Integer")) {
				if (i == 0) {
					col1min = Integer.parseInt(htblColNameMin.get(columnnames[0]));
					col1max = Integer.parseInt(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Integer.parseInt(htblColNameMin.get(columnnames[1]));
					col2max = Integer.parseInt(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Integer.parseInt(htblColNameMin.get(columnnames[2]));
					col3max = Integer.parseInt(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.String")) {
				if (i == 0) {
					col1min = htblColNameMin.get(columnnames[0]);
					col1max = htblColNameMax.get(columnnames[0]);
				} else if (i == 1) {
					col2min = htblColNameMin.get(columnnames[1]);
					col2max = htblColNameMax.get(columnnames[1]);
				} else if (i == 2) {
					col3min = htblColNameMin.get(columnnames[2]);
					col3max = htblColNameMax.get(columnnames[2]);
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.Double")) {
				if (i == 0) {
					col1min = Double.parseDouble(htblColNameMin.get(columnnames[0]));
					col1max = Double.parseDouble(htblColNameMax.get(columnnames[0]));
				} else if (i == 1) {
					col2min = Double.parseDouble(htblColNameMin.get(columnnames[1]));
					col2max = Double.parseDouble(htblColNameMax.get(columnnames[1]));
				} else if (i == 2) {
					col3min = Double.parseDouble(htblColNameMin.get(columnnames[2]));
					col3max = Double.parseDouble(htblColNameMax.get(columnnames[2]));
				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				if (i == 0) {
					try {
						col1min = formatter.parse(htblColNameMin.get(columnnames[0]));
						col1max = formatter.parse(htblColNameMax.get(columnnames[0]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 1) {
					try {
						col2min = formatter.parse(htblColNameMin.get(columnnames[1]));
						col2max = formatter.parse(htblColNameMax.get(columnnames[1]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 2) {
					try {
						col3min = formatter.parse(htblColNameMin.get(columnnames[2]));
						col3max = formatter.parse(htblColNameMax.get(columnnames[2]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}

		}
		Object[] midVal = null;
		if (levelargument == 0) {
			midVal = computeMidVal3Col(col1min, col1max,
					col2min, col2max,
					col3min, col3max);
		} else {
			midVal = computeMidVal3Col2(col1min, col1max, midValarrgument[0],
					col2min, col2max, midValarrgument[1],
					col3min, col3max, midValarrgument[2], resultarrgument);
		}

		// check if the node isleaf
		if (this.children == null) {
			// check for the correct address and delete it
			for (int i = 0; i < this.refrences.size(); i++) {
				Hashtable<String, Object> curraddressdata = this.refrences.get(i).actualvalues;
				int currpagenum = this.refrences.get(i).pagenum;
				boolean flag = true;
				for (int j = 0; j < columnnames.length; j++) {
					if (curraddressdata.containsKey(columnnames[j])) {
						if (!curraddressdata.get(columnnames[j]).equals(actualvalues.get(columnnames[j]))) {
							flag = false;
							break;
						}
					} else {
						flag = false;
						break;
					}
				}
				if (flag && currpagenum == page_number) {
					this.refrences.remove(i);
					return;
				}
			}

			return;

		} else {
			// if the node is not leaf
			// get the index of the child that the row should be inserted in
			String index = getnodenum(midVal, row, columnnames);
			// insert the row in the child
			int nodenum = Integer.parseInt(index, 2);
			Node newchNode = this.children.get(nodenum);
			this.children.get(nodenum).deleteforupdate(row, actualvalues, htblColNameType, htblColNameMin,
					htblColNameMax, levelargument, midValarrgument, resultarrgument, page_number);
			return;

		}
	}

	public Vector<Integer> findRows(Hashtable<String, Object> values, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			int levelargument, Object[] midValarrgument, String resultarrgument, Vector<Integer> returnpagenumber)
			throws DBAppException {
		// get the middle value for 3 columns
		Object col1min = null;
		Object col1max = null;
		Object col2min = null;
		Object col2max = null;
		Object col3min = null;
		Object col3max = null;

		Row row = new Row(values);

		for (int i = 0; i < columnnames.length; i++) {
			if (htblColNameType.get(columnnames[i]).equals("java.lang.Integer")) {
				if (i == 0) {
					col1min = Integer.parseInt(htblColNameMin.get(columnnames[0]));
					col1max = Integer.parseInt(htblColNameMax.get(columnnames[0]));

				} else if (i == 1) {
					col2min = Integer.parseInt(htblColNameMin.get(columnnames[1]));
					col2max = Integer.parseInt(htblColNameMax.get(columnnames[1]));

				} else if (i == 2) {
					col3min = Integer.parseInt(htblColNameMin.get(columnnames[2]));
					col3max = Integer.parseInt(htblColNameMax.get(columnnames[2]));

				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.String")) {
				if (i == 0) {
					col1min = htblColNameMin.get(columnnames[0]);
					col1max = htblColNameMax.get(columnnames[0]);

				} else if (i == 1) {
					col2min = htblColNameMin.get(columnnames[1]);
					col2max = htblColNameMax.get(columnnames[1]);

				} else if (i == 2) {
					col3min = htblColNameMin.get(columnnames[2]);
					col3max = htblColNameMax.get(columnnames[2]);

				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.lang.Double")) {
				if (i == 0) {
					col1min = Double.parseDouble(htblColNameMin.get(columnnames[0]));
					col1max = Double.parseDouble(htblColNameMax.get(columnnames[0]));

				} else if (i == 1) {
					col2min = Double.parseDouble(htblColNameMin.get(columnnames[1]));
					col2max = Double.parseDouble(htblColNameMax.get(columnnames[1]));

				} else if (i == 2) {
					col3min = Double.parseDouble(htblColNameMin.get(columnnames[2]));
					col3max = Double.parseDouble(htblColNameMax.get(columnnames[2]));

				}
			} else if (htblColNameType.get(columnnames[i]).equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				if (i == 0) {
					try {
						col1min = formatter.parse(htblColNameMin.get(columnnames[0]));
						col1max = formatter.parse(htblColNameMax.get(columnnames[0]));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 1) {
					try {
						col2min = formatter.parse(htblColNameMin.get(columnnames[1]));
						col2max = formatter.parse(htblColNameMax.get(columnnames[1]));

					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (i == 2) {
					try {
						col3min = formatter.parse(htblColNameMin.get(columnnames[2]));
						col3max = formatter.parse(htblColNameMax.get(columnnames[2]));
						
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}

		}
		// System.out.println("min: " + col1min + " max: " + col1max);
		Object[] midVal = null;
		if (levelargument == 0) {
			midVal = computeMidVal3Col(col1min, col1max,
					col2min, col2max,
					col3min, col3max);
		} else {
			midVal = computeMidVal3Col2(col1min, col1max, midValarrgument[0],
					col2min, col2max, midValarrgument[1],
					col3min, col3max, midValarrgument[2], resultarrgument);
		}

		// check if the node isleaf
		if (this.children == null) {
			// check for the correct address and get the page number add it to
			// returnpagenumber
			for (int i = 0; i < this.refrences.size(); i++) {
				Hashtable<String, Object> curraddress = this.refrences.get(i).actualvalues;
				boolean flag = true;
				for (int j = 0; j < columnnames.length; j++) {
					if (curraddress.containsKey(columnnames[j])) {
						if (!curraddress.get(columnnames[j]).equals(values.get(columnnames[j]))) {
							flag = false;
							break;
						}
					} else {
						flag = false;
						break;
					}
				}
				if (flag) {
					returnpagenumber.add(this.refrences.get(i).pagenum);
				}

			}
			if (returnpagenumber.size() == 0) {
				throw new DBAppException("The row does not exist");
			}
			return returnpagenumber;

		} else {
			// if the node is not leaf
			// get the index of the child that the row should be inserted in
			String index = getnodenum(midVal, row, columnnames);
			// insert the row in the child
			int nodenum = Integer.parseInt(index, 2);
			Node newchNode = this.children.get(nodenum);
			returnpagenumber = this.children.get(nodenum).findRows(values, htblColNameType,
					htblColNameMin, htblColNameMax, newchNode.level, midVal, index, returnpagenumber);
			return returnpagenumber;

		}
	}
}
