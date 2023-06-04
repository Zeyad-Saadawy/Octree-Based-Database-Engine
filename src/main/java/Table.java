import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {
	String clusteringKeyColName;
	String clusteringKeyColType;
	String tableName;
	Hashtable<String, String> htblColNameType;
	Hashtable<String, String> htblColNameMin;
	Hashtable<String, String> htblColNameMax;
	Vector<PageInfo> pageInfos;
	int n;
	boolean hasindex;
	Vector<Node> indices;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax)
			throws FileNotFoundException {
		this.tableName = strTableName;
		this.clusteringKeyColName = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		this.clusteringKeyColType = htblColNameType.get(strClusteringKeyColumn);
		this.n = readMaximumRowsCountinTablePage();

		if (pageInfos == null) {
			pageInfos = new Vector<PageInfo>();
		}
		hasindex = false;
		this.indices = new Vector<Node>();

	}

	public static int readMaximumRowsCountinTablePage() throws FileNotFoundException {
		BufferedReader reader;

		try {
			reader = new BufferedReader(new FileReader("src/main/resources/DBApp.config"));
			String line = reader.readLine();

			while (line != null) {
				String[] parts = line.split("=");
				if (parts[0].equals("MaximumRowsCountinTablePage ")) {
					reader.close();
					return Integer.parseInt(parts[1].replaceAll(" ", ""));
				}

				line = reader.readLine();
			}
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("File does not exist.");

		}
		return 0;

	}

	public static void serializePage(Vector<Row> data, String pageName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + pageName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(data);
			out.reset();
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved in resources/data/" + pageName +
			// ".ser");
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

	public void createNewPage() {
		// makes a new page file and adds an empty vector object
		if (pageInfos.size() > 0) {
			PageInfo last_page = pageInfos.lastElement();
			int page_number = pageInfos.size();
			String page_name = tableName + "_" + page_number;
			Vector<Row> new_page = new Vector<Row>();
			Object new_min = last_page.max;
			PageInfo new_page_info = new PageInfo(clusteringKeyColName, new_min, new_min);
			pageInfos.add(new_page_info);
			serializePage(new_page, page_name);
		} else {
			int page_number = 0;
			String page_name = tableName + "_" + page_number;
			Vector<Row> new_page = new Vector<Row>();
			Object new_min = htblColNameMin.get(clusteringKeyColName);
			PageInfo new_page_info = new PageInfo(clusteringKeyColName, new_min, new_min);
			pageInfos.add(new_page_info);
			serializePage(new_page, page_name);
		}

	}

	public Vector<Row> sortPage(Vector<Row> page_data) {
		// returns the sorted page data
		Comparator<Row> comparator = null;
		if (clusteringKeyColType.equals("java.lang.Integer")) {
			comparator = (Row r1, Row r2) -> {
				Integer v1 = (Integer) r1.values.get(clusteringKeyColName);
				Integer v2 = (Integer) r2.values.get(clusteringKeyColName);
				if (v1 == null || v2 == null) {
					return 0; // handle null values as equal
				}
				return v1 - v2;
			};
		} else if (clusteringKeyColType.equals("java.lang.Double")) {
			comparator = (Row r1, Row r2) -> {
				Double v1 = (Double) r1.values.get(clusteringKeyColName);
				Double v2 = (Double) r2.values.get(clusteringKeyColName);
				if (v1 == null || v2 == null) {
					return 0; // handle null values as equal
				}
				return Double.compare(v1, v2);
			};
		} else if (clusteringKeyColType.equals("java.lang.String")) {
			comparator = (Row r1, Row r2) -> {
				String v1 = (String) r1.values.get(clusteringKeyColName);
				String v2 = (String) r2.values.get(clusteringKeyColName);
				if (v1 == null && v2 == null) {
					return 0; // handle null values as equal
				} else if (v1 == null) {
					return -1; // handle null as less than non-null
				} else if (v2 == null) {
					return 1; // handle non-null as greater than null
				}
				return v1.compareTo(v2);
			};
		} else if (clusteringKeyColType.equals("java.util.Date")) {
			comparator = (Row r1, Row r2) -> {
				Date v1 = (Date) r1.values.get(clusteringKeyColName);
				Date v2 = (Date) r2.values.get(clusteringKeyColName);
				if (v1 == null || v2 == null) {
					return 0; // handle null values as equal
				}
				return v1.compareTo(v2);
			};
		}
		if (comparator != null) {
			page_data.sort(comparator);
		}

		return page_data;
	}

	public void updatePageInfo(int pageNum) throws IOException {
		Vector<Row> page = deserializePage(tableName + '_' + pageNum);
		page = sortPage(page);

		if (clusteringKeyColType.equals("java.lang.Integer")) {
			int min = (int) page.get(0).values.get(clusteringKeyColName);
			int max = (int) page.get(page.size() - 1).values.get(clusteringKeyColName);
			pageInfos.get(pageNum).min = min;
			pageInfos.get(pageNum).max = max;
		} else if (clusteringKeyColType.equals("java.lang.String")) {
			pageInfos.get(pageNum).min = (String) page.get(0).values.get(clusteringKeyColName);
			pageInfos.get(pageNum).max = (String) page.get(page.size() - 1).values.get(clusteringKeyColName);
		} else if (clusteringKeyColType.equals("java.lang.Double")) {
			double min = (double) page.get(0).values.get(clusteringKeyColName);
			double max = (double) page.get(page.size() - 1).values.get(clusteringKeyColName);
			pageInfos.get(pageNum).min = min;
			pageInfos.get(pageNum).max = max;
		} else if (clusteringKeyColType.equals("java.util.Date")) {
			pageInfos.get(pageNum).min = (Date) page.get(0).values.get(clusteringKeyColName);
			pageInfos.get(pageNum).max = (Date) page.get(page.size() - 1).values.get(clusteringKeyColName);
		}
		pageInfos.get(pageNum).capacity = page.size();
		serializePage(page, tableName + '_' + pageNum);
	}

	public int findPage(Row row) throws IOException {
		// get clustering key

		if (clusteringKeyColType.equals("java.lang.Integer")) {
			int clusteringKeyValue = (int) row.values.get(clusteringKeyColName);
			for (int i = 0; i < this.pageInfos.size(); i++) {
				if (i == this.pageInfos.size() - 1)
					return i;
				else if (clusteringKeyValue >= (int) this.pageInfos.get(i).min
						&& clusteringKeyValue <= (int) this.pageInfos.get(i).max)
					return i;
				else if (this.pageInfos.get(i).capacity < n && clusteringKeyValue < (int) this.pageInfos.get(i + 1).min
						&& clusteringKeyValue > (int) this.pageInfos.get(i).max)
					return i;
				else if (this.pageInfos.get(i).capacity == n && clusteringKeyValue > (int) this.pageInfos.get(i).max
						&& clusteringKeyValue < (int) this.pageInfos.get(i + 1).min
						&& this.pageInfos.get(i + 1).capacity < n)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue > (int) this.pageInfos.get(i).max
						&& clusteringKeyValue < (int) this.pageInfos.get(i + 1).min)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue < (int) this.pageInfos.get(i).min)
					return i;

			}
		} else if (clusteringKeyColType.equals("java.lang.Double")) {
			double clusteringKeyValue = (double) row.values.get(clusteringKeyColName);
			for (int i = 0; i < this.pageInfos.size(); i++) {
				if (i == this.pageInfos.size() - 1)
					return i;
				else if (clusteringKeyValue >= (double) this.pageInfos.get(i).min
						&& clusteringKeyValue <= (double) this.pageInfos.get(i).max)
					return i;
				else if (this.pageInfos.get(i).capacity < n
						&& clusteringKeyValue < (double) this.pageInfos.get(i + 1).min
						&& clusteringKeyValue > (double) this.pageInfos.get(i).max)
					return i;
				else if (this.pageInfos.get(i).capacity == n && clusteringKeyValue > (double) this.pageInfos.get(i).max
						&& clusteringKeyValue < (double) this.pageInfos.get(i + 1).min
						&& this.pageInfos.get(i + 1).capacity < n)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue > (double) this.pageInfos.get(i).max
						&& clusteringKeyValue < (double) this.pageInfos.get(i + 1).min)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue < (double) this.pageInfos.get(i).min)
					return i;

			}
		} else if (clusteringKeyColType.equals("java.lang.String")) {
			String clusteringKeyValue = (String) row.values.get(clusteringKeyColName);
			for (int i = 0; i < this.pageInfos.size(); i++) {
				if (i == this.pageInfos.size() - 1)
					return i;
				else if (clusteringKeyValue.compareTo((String) this.pageInfos.get(i).min) >= 0
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i).max) <= 0)
					return i;
				else if (this.pageInfos.get(i).capacity < n
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i + 1).min) < 0
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i).max) > 0)
					return i;
				else if (this.pageInfos.get(i).capacity == n
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i + 1).min) < 0
						&& this.pageInfos.get(i + 1).capacity < n)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i).max) > 0
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i + 1).min) < 0)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue.compareTo((String) this.pageInfos.get(i).min) < 0)
					return i;

			}
		} else if (clusteringKeyColType.equals("java.util.Date")) {
			Date clusteringKeyValue = (Date) row.values.get(clusteringKeyColName);
			for (int i = 0; i < this.pageInfos.size(); i++) {
				if (i == this.pageInfos.size() - 1)
					return i;
				else if (clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).min) >= 0
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).max) <= 0)
					return i;
				else if (this.pageInfos.get(i).capacity < n
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i + 1).min) < 0
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).max) > 0)
					return i;
				else if (this.pageInfos.get(i).capacity == n
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).max) > 0
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i + 1).min) < 0
						&& this.pageInfos.get(i + 1).capacity < n)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).max) > 0
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i + 1).min) < 0)
					return i + 1;
				else if (this.pageInfos.get(i).capacity == n && this.pageInfos.get(i + 1).capacity == n
						&& clusteringKeyValue.compareTo((Date) this.pageInfos.get(i).min) < 0)
					return i;
			}
		}

		createNewPage();
		return pageInfos.size() - 1;

	}

	public void insertIntoPage(Row row) throws IOException, DBAppException {
		int pageNum = findPage(row);
		if (pageNum == 0 && this.pageInfos.size() == 0) {
			createNewPage();
		} else {
			// check if row is already in the table
			Vector<Row> pagecheck = deserializePage(tableName + '_' + pageNum);
			for (int i = 0; i < pagecheck.size(); i++) {
				if (pagecheck.get(i).equals(row)) {
					throw new DBAppException("Row is already in the table");
				}
			}
			// check if the row's clustering key is already in the table
			for (int i = 0; i < pagecheck.size(); i++) {
				if (pagecheck.get(i).values.get(clusteringKeyColName)
						.equals(row.values.get(clusteringKeyColName))) {
					throw new DBAppException("Row's clustering key is already in the table");
				}
			}
			serializePage(pagecheck, tableName + '_' + pageNum);

			// check if this page has space
			if (pageInfos.get(pageNum).capacity < n) {
				Vector<Row> page = deserializePage(tableName + '_' + pageNum);
				page.add(row);
				serializePage(page, tableName + '_' + pageNum);
				// update page info
				updatePageInfo(pageNum);
				// sort el page
				page = sortPage(page);
				// serialize page
				serializePage(page, tableName + '_' + pageNum);

			} else {
				// split page

				splitPage(pageNum, row);

			}
		}

		// insert into indices
		if (hasindex) {
			int rowindex = -1;
			for (int i = 0; i < pageInfos.size(); i++) {
				Vector<Row> page = deserializePage(tableName + '_' + i);
				for (int j = 0; j < page.size(); j++) {
					if (page.get(j).values.get(clusteringKeyColName).equals(row.values.get(clusteringKeyColName))) {
						rowindex = j;
						break;
					}
				}
				if (rowindex != -1) {
					serializePage(page, tableName + '_' + i);
					pageNum = i;
					break;
				}
				serializePage(page, tableName);
			}

			Address addr = null;
			for (int i = 0; i < indices.size(); i++) {
				Object[] midValarrgument = null;
				Node idx = indices.get(i);
				// check if the index columns are not null in the row
				boolean flag = true;
				for (int j = 0; j < idx.columnnames.length; j++) {
					if (row.values.get(idx.columnnames[i]) == null) {
						flag = false;
						break;
					}

				}
				if (flag) {
					String col1name = idx.columnnames[0];
					String col2name = idx.columnnames[1];
					String col3name = idx.columnnames[2];
					Object col1value = row.values.get(col1name);
					Object col2value = row.values.get(col2name);
					Object col3value = row.values.get(col3name);
					Object clusteringKeyValue = row.values.get(clusteringKeyColName);
					Hashtable<String, Object> actualvalues = new Hashtable<String, Object>();
					actualvalues.put(col1name, col1value);
					actualvalues.put(col2name, col2value);
					actualvalues.put(col3name, col3value);

					addr = new Address(pageNum, tableName, actualvalues, clusteringKeyValue);
					idx.insertintoIndex(row, addr, htblColNameType, htblColNameMin, htblColNameMax,
							0, midValarrgument, "");
				} else {
					throw new DBAppException("Index columns are not null");
				}
			}

		}

	}

	public void splitPage(int pageNum, Row row) throws IOException, DBAppException {
		// get page
		Vector<Row> page = deserializePage(tableName + '_' + pageNum);
		// add the row and now the page is maximum size +1
		page.add(row);
		// sort the page
		page = sortPage(page);
		// get the last row
		Row lastRow = page.get(page.size() - 1);
		// remove the last row to have n rows only
		page.remove(page.size() - 1);
		// serialize page
		serializePage(page, tableName + '_' + pageNum);
		// update page info sort page, update page capacity
		updatePageInfo(pageNum);
		// get the next page to insert that row in it
		int nextpageNum = pageNum + 1;
		Vector<Row> nextPage = null;
		// law kanet akher page fa ha3mel wahda gededa w ahot feha eloverflow
		if (nextpageNum == pageInfos.size()) {
			createNewPage();
			// insert the last row in the new page
			nextPage = deserializePage(tableName + '_' + (nextpageNum));
			nextPage.add(lastRow);
			for (int i = 0; i < indices.size(); i++) {
				Object[] midValarrgument = null;

				indices.get(i).updatepagenum(lastRow, lastRow.values, htblColNameType, htblColNameMin, htblColNameMax,
						0, midValarrgument, "", pageNum, nextpageNum);
			}
			// serialize page
			serializePage(nextPage, tableName + '_' + nextpageNum);
			// update page info
			updatePageInfo(nextpageNum);
			// mesh mehtaga sort heya kda kda new w mafhash gher el record da
		} else {
			while (nextpageNum < pageInfos.size()) {
				// deserialize the next page
				nextPage = deserializePage(tableName + '_' + nextpageNum);
				// add the last row in the next page
				nextPage.add(lastRow);
				for (int i = 0; i < indices.size(); i++) {
					Object[] midValarrgument = null;
					indices.get(i).updatepagenum(lastRow, lastRow.values, htblColNameType, htblColNameMin,
							htblColNameMax,
							0, midValarrgument, "", pageNum, nextpageNum);
				}
				// sort the next page
				nextPage = sortPage(nextPage);
				// check if the this page is not full to break.. if full continue
				if (nextPage.size() <= n) {
					// serialize page
					serializePage(nextPage, tableName + '_' + nextpageNum);
					// update page info
					updatePageInfo(nextpageNum);
					break;
				}
				// get the last row
				lastRow = nextPage.get(nextPage.size() - 1);
				// remove the last row
				nextPage.remove(nextPage.size() - 1);
				// serialize page
				serializePage(nextPage, tableName + '_' + nextpageNum);
				// update page info
				updatePageInfo(nextpageNum);
				// increment the next page number
				nextpageNum++;
				if (nextpageNum == pageInfos.size()) {
					createNewPage();
					// insert the last row in the new page
					nextPage = deserializePage(tableName + '_' + (nextpageNum));
					nextPage.add(lastRow);
					for (int i = 0; i < indices.size(); i++) {
						Object[] midValarrgument = null;
						indices.get(i).updatepagenum(lastRow, lastRow.values, htblColNameType, htblColNameMin,
								htblColNameMax,
								0, midValarrgument, "", pageNum, nextpageNum);
					}
					// serialize page
					serializePage(nextPage, tableName + '_' + nextpageNum);
					// update page info
					updatePageInfo(nextpageNum);
					// mesh mehtaga sort heya kda kda new w mafhash gher el record da
					break;
				}
			}
		}
	}

	public void deletePage(int pageNum) throws IOException {
		// delete page
		File file = new File(tableName + '_' + pageNum);
		file.delete();
		// delete page info
		pageInfos.remove(pageNum);

		// change all pages after it
		for (int i = pageNum; i < pageInfos.size(); i++) {
			File file2 = new File(tableName + '_' + (i + 1));
			file2.renameTo(new File(tableName + '_' + i));
		}
	}

	public void delete(Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {
		boolean octway = false;
		int indexnum = -1;
		// loop on indices and get column names of indices and check if the column names
		// of the hashtable are in the indices
		for (int i = 0; i < indices.size(); i++) {
			Node idx = indices.get(i);
			// check if htblecolnamevalue contains the column names of the index
			boolean flag = true;
			for (int j = 0; j < idx.columnnames.length; j++) {
				if (!htblColNameValue.containsKey(idx.columnnames[j])) {
					flag = false;
					break;
				}
			}
			if (flag) {
				octway = true;
				indexnum = i;
				break;
			}

		}
		if (octway) {
			Row newrow = new Row(htblColNameValue);
			Node root = indices.get(indexnum);
			Row row = new Row(htblColNameValue);
			Vector<Integer> noneed = new Vector<Integer>();
			Vector<Integer> pages = root.deletefromIndex(row, htblColNameValue, htblColNameType, htblColNameMin,
					htblColNameMax,
					0, null, "", noneed);
			// get unique pages from paages vector
			Vector<Integer> uniquepages = new Vector<Integer>();
			for (int i = 0; i < pages.size(); i++) {
				if (!uniquepages.contains(pages.get(i))) {
					uniquepages.add(pages.get(i));
				}
			}
			pages = uniquepages;
			for (int i = 0; i < pages.size(); i++) {

				Vector<Row> page_data = deserializePage(tableName + '_' + pages.get(i));

				for (int j = 0; j < page_data.size(); j++) {
					Boolean should_be_deleted = true;
					Row row2 = page_data.get(j);

					for (Object key : htblColNameValue.keySet()) {
						if (row2.values.get(key) == null) {
							throw new DBAppException("Column " + key + " does not exist in table " + tableName);
						}
						if (!htblColNameValue.get(key).equals(row2.values.get(key))) {
							should_be_deleted = false;
							break;
						}
					}
					if (should_be_deleted) {
						newrow = row2;
						page_data.remove(j);
						j--;
					}
					// check if we reach the end of the page then break
					if (j >= page_data.size() - 1) {
						break;
					}
				}
				if (page_data.size() == 0) {
					deletePage(i);
					i--;
				} else {
					serializePage(page_data, tableName + '_' + i);
					updatePageInfo(i);
				}
			}
			Object[] noneed1 = null;
			Vector<Integer> noneed2 = new Vector<Integer>();
			for (int i = 0; i < indices.size(); i++) {
				String[] coln = indices.get(i).columnnames;
				// check if there is any common column in the row values hashtable and the index
				// column name
				boolean flag = false;
				System.out.println(i + "----i");
				for (int j = 0; j < coln.length; j++) {
					System.out.println(coln[j] + "----coln[j]" + "   i" + i);
					if (newrow.values.containsKey(coln[j])) {
						flag = true;
						System.out.println("flag is true");
						break;
					}

				}
				if (!flag) {
					continue;
				}
				Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
				for (int j = 0; j < coln.length; j++) {
					htblColNameValue2.put(coln[j], newrow.values.get(coln[j]));
				}
				indices.get(i).deletefromIndex(newrow, htblColNameValue2, htblColNameType, htblColNameMin,
						htblColNameMax,
						0, noneed1, "", noneed2);
			}
		} else {

			for (int i = 0; i < pageInfos.size(); i++) {
				Vector<Row> page_data = deserializePage(tableName + '_' + i);

				for (int j = 0; j < page_data.size(); j++) {
					Boolean should_be_deleted = true;
					Row row = page_data.get(j);
					for (Object key : htblColNameValue.keySet()) {
						if (row.values.get(key) == null) {
							throw new DBAppException("Column " + key + " does not exist in table " + tableName);
						}
						if (!htblColNameValue.get(key).equals(row.values.get(key))) {
							should_be_deleted = false;
							break;
						}
					}
					if (should_be_deleted) {
						page_data.remove(j);
						j--;
					}

					// check if we reach the end of the page then break
					if (j >= page_data.size() - 1) {
						break;
					}
				}
				if (page_data.size() == 0) {
					deletePage(i);
					i--;
				} else {
					serializePage(page_data, tableName + '_' + i);
					updatePageInfo(i);
				}
				// check if we reach the end then break
				if (i >= pageInfos.size() - 1) {
					break;
				}

			}

		}
	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> htblColNameValueargument)
			throws IOException, DBAppException {
		boolean octway = false;
		Vector<Integer> indexnum = new Vector<Integer>();
		if (hasindex) {

			for (int i = 0; i < indices.size(); i++) {
				Node idx = indices.get(i);
				// check if row hashtable contains any column of column names of the index
				boolean flag = false;
				for (int j = 0; j < idx.columnnames.length; j++) {
					if (htblColNameValueargument.containsKey(idx.columnnames[j])) {
						flag = true;
						break;
					}
				}
				if (flag) {
					octway = true;
					indexnum.add(i);
				}

			}
		}
		// get the page number of the row
		System.out.println(clusteringKeyValue);
		int pageNum = getPageNum(clusteringKeyValue);
		// get the page
		Vector<Row> page = deserializePage(tableName + "_" + pageNum);
		// update the row
		for (int j = 0; j < page.size(); j++) {
			// printThisPage(page);
			System.out.println("Value: " + page.get(j).values.get(clusteringKeyColName));
			if (page.get(j).values.get(clusteringKeyColName).toString().equals(clusteringKeyValue)) {
				Row oldrow = page.get(j);
				//create new hashtable of oldrow values
				Hashtable<String, Object> oldrowvalues = new Hashtable<String, Object>();
				Set<String> setOfKeys2 = oldrow.values.keySet();
				for (String key : setOfKeys2) {
					oldrowvalues.put(key, oldrow.values.get(key));
				}

				Row row2 = new Row(oldrowvalues);
				// if octway we will delete from index that address and insert it again to the
				// index
				if (octway) {
					for (int i = 0; i < indices.size(); i++) {
						Node idx = indices.get(i);
						// delete from index
						Object[] noneed = new Object[0];
						idx.deleteforupdate(row2, row2.values, htblColNameType, htblColNameMin, htblColNameMax,
								0, noneed, "", pageNum);
						// update the row
						Set<String> setOfKeys = htblColNameValueargument.keySet();
						for (String key : setOfKeys) {
							oldrow.values.put(key, htblColNameValueargument.get(key));
						}
						// insert to index
						Hashtable<String, Object> newvalues = new Hashtable<String, Object>();
						for (int k = 0; k < idx.columnnames.length; k++) {
							newvalues.put(idx.columnnames[k], oldrow.values.get(idx.columnnames[k]));
						}
						Address newaddress = new Address(pageNum, tableName, newvalues, clusteringKeyValue);
						idx.insertintoIndex(oldrow, newaddress, htblColNameType, htblColNameMin, htblColNameMax,
								0, noneed, "");
					}
				}
				// if not octway we will update the row
				else {
					Set<String> setOfKeys = htblColNameValueargument.keySet();
					for (String key : setOfKeys) {
						oldrow.values.put(key, htblColNameValueargument.get(key));
					}
				}

			}
		}
		serializePage(page, tableName + "_" + pageNum);
		return;

	}

	private int getPageNum(String clusteringKeyValue) throws DBAppException {
		int pageNum = 0;

		for (int i = 0; i < pageInfos.size(); i++) {
			PageInfo page = pageInfos.get(i);
			if (clusteringKeyColType.equals("java.lang.Integer")) {
				if (Integer.parseInt(clusteringKeyValue) >= Integer.parseInt(page.min.toString())
						&& Integer.parseInt(clusteringKeyValue) <= Integer.parseInt(page.max.toString())) {
					pageNum = i;
					break;
				}
			} else if (clusteringKeyColType.equals("java.lang.String")) {

				if ((clusteringKeyValue).compareTo(page.min.toString()) >= 0
						&& (clusteringKeyValue).compareTo(page.max.toString()) <= 0) {
					pageNum = i;
					break;
				}
			} else if (clusteringKeyColType.equals("java.lang.Double")) {
				if (Double.parseDouble(clusteringKeyValue) >= Double.parseDouble(page.min.toString())
						&& Double.parseDouble(clusteringKeyValue) <= Double.parseDouble(page.max.toString())) {
					pageNum = i;
					break;
				}
			} else if (clusteringKeyColType.equals("java.util.Date")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				try {

					Date date1 = formatter.parse(clusteringKeyValue);
					Date date2 = (Date) page.min;
					Date date3 = (Date) page.max;
					if (date1.compareTo(date2) >= 0 && date1.compareTo(date3) <= 0) {
						pageNum = i;
						break;
					}
				} catch (ParseException e) {
					throw new DBAppException("Invalid date format" + e.getMessage());
				}
			}
		}

		return pageNum;
	}

	public void printAllPages() {
		for (int i = 0; i < pageInfos.size(); i++) {
			Vector<Row> page_data = deserializePage(tableName + '_' + i);
			System.out.println("Page " + i + ":");
			for (int j = 0; j < page_data.size(); j++) {
				System.out.println(page_data.get(j).values);
			}
			serializePage(page_data, tableName + '_' + i);
		}
	}

	public void printindex() {
		// print each index inside indices vector
		// printAllPages();

		System.out.println(" indices size" + indices.size());
		System.out.println(" pageinfos size" + pageInfos.size());
		for (int i = 0; i < indices.size(); i++) {
			// print each index
			indices.get(i).print();
			System.out.println("----------end idx ----------");

		}

	}

	public void fillindex(String[] strarrColName, String indexname) throws DBAppException {
		Node root = null;
		for (int l = 0; l < indices.size(); l++) {
			if (indices.get(l).indexname == indexname) {
				root = indices.get(l);
			}
		}
		for (int i = 0; i < pageInfos.size(); i++) {
			Vector<Row> page = deserializePage(tableName + '_' + i);
			Address addr = null;
			for (int j = 0; j < page.size(); j++) {
				Row row = page.get(j);
				Object[] midValarrgument = null;

				String col1name = strarrColName[0];
				String col2name = strarrColName[1];
				String col3name = strarrColName[2];
				Object col1value = row.values.get(strarrColName[0]);
				Object col2value = row.values.get(strarrColName[1]);
				Object col3value = row.values.get(strarrColName[2]);
				Object clusteringkey = row.values.get(clusteringKeyColName);
				Hashtable<String, Object> actualvalues = new Hashtable<String, Object>();
				actualvalues.put(col1name, col1value);
				actualvalues.put(col2name, col2value);
				actualvalues.put(col3name, col3value);

				addr = new Address(i, tableName, actualvalues, clusteringkey);
				root.insertintoIndex(row, addr, htblColNameType, htblColNameMin, htblColNameMax,
						0, midValarrgument, "");

			}
			serializePage(page, tableName + '_' + i);
		}

	}

}
