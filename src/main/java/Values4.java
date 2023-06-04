import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;


public class Values4 {

	public static void main(String[] args) throws DBAppException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		dbApp.init();		
		creating(strTableName, dbApp);


				String[] strarrColName= {"x","gpa","phone"};
		try {
			dbApp.createIndex(strTableName, strarrColName);
		} catch ( DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		inserting(dbApp);
//		


		
		
//		dbApp.getPages("Student");
		// dbApp.printindex(strTableName);
		// System.out.println("------------------");
		// deleting(strTableName,dbApp);
//		dbApp.getPages("Student");
//		System.out.println("mainmethod");
	}


	private static void deleting(String strTableName, DBApp dbApp) throws DBAppException{
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();

		rec.put("x", new Integer(2));

		rec.put("gpa", new Integer(3));

		rec.put("phone", new Integer(6));
		dbApp.deleteFromTable(strTableName,rec);
		dbApp.printindex(strTableName);
	}


	private static void inserting(DBApp dbApp) throws DBAppException {
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(10));
		rec.put("x", new Integer(1));
		rec.put("name", new String("A"));
		rec.put("gpa", new Integer(2));
		rec.put("Date of Birth", new Date(2015-1900,9-1,17));
		rec.put("phone", new Integer(3));
		dbApp.insertIntoTable("Student", rec);
		dbApp.getPages("Student");
		dbApp.printindex("Student");

		rec.clear();
		rec.put("id", new Integer(0));
		rec.put("x", new Integer(2));
		rec.put("name", new String("A"));
		rec.put("gpa", new Integer(3));
		rec.put("Date of Birth", new Date(2015-1900,9-1,17));
		rec.put("phone", new Integer(6));
		dbApp.insertIntoTable("Student", rec);
		dbApp.getPages("Student");
		dbApp.printindex("Student");
		
// 		rec.clear();
// 		rec.put("id", new Integer(1));
// 		rec.put("x", new Integer(6));
// 		rec.put("name", new String("B"));
// 		rec.put("gpa", new Integer(3));
// 		rec.put("Date of Birth", new Date(2040-1900,9-1,17));
// 		rec.put("phone", new Integer(5));
// 		dbApp.insertIntoTable("Student", rec);
		
// //		dbApp.getPages("Student");

// 		rec.clear();

// 		rec.put("id", new Integer(2));
// 		rec.put("x", new Integer(2));
// 		rec.put("name", new String("C"));
// 		rec.put("gpa", new Integer(3));
// 		rec.put("Date of Birth", new Date(2015-1900,9-1,26));
// 		rec.put("phone", new Integer(25));

// 		dbApp.insertIntoTable("Student", rec);
// 		rec.clear();

// 		rec.put("id", new Integer(3));
// 		rec.put("x", new Integer(2));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(13));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(6));

// 		dbApp.insertIntoTable("Student", rec);
// 		rec.clear();

// 		rec.put("id", new Integer(4));
// 		rec.put("x", new Integer(7));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(3));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(27));

// 		dbApp.insertIntoTable("Student", rec);
		
// 		rec.clear();

// 		rec.put("id", new Integer(5));
// 		rec.put("x", new Integer(7));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(3));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(28));

// 		dbApp.insertIntoTable("Student", rec);
		
// 		rec.clear();

// 		rec.put("id", new Integer(6));
// 		rec.put("x", new Integer(2));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(3));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(9));

// 		dbApp.insertIntoTable("Student", rec);
		
// 		rec.clear();

// 		rec.put("id", new Integer(7));
// 		rec.put("x", new Integer(1));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(8));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(2));

// 		dbApp.insertIntoTable("Student", rec);
		
// 		rec.clear();

// 		rec.put("id", new Integer(8));
// 		rec.put("x", new Integer(7));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(11));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(26));

// 		dbApp.insertIntoTable("Student", rec);
		
// 		rec.clear();
// //
// 		rec.put("id", new Integer(9));
// 		rec.put("x", new Integer(4));
// 		rec.put("name", new String("A"));
// 		rec.put("gpa", new Integer(9));
// 		rec.put("Date of Birth", new Date(2040-1900,11-1,26));
// 		rec.put("phone", new Integer(18));

// 		dbApp.insertIntoTable("Student", rec);

//		rec.clear();
//
//		rec.put("name", new String("A"));
//		rec.put("id", new Integer(4));
//		rec.put("gpa", 1);
//		rec.put("Date of Birth", new Date(2040-1900,10-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//		
//		rec.clear();
//		
//		rec.put("name", new String("A"));
//		rec.put("id", new Integer(5));
//		rec.put("gpa", 700);
//		rec.put("Date of Birth", new Date(2040-1900,10-1,28));
//
//		dbApp.insertIntoTable("Student", rec);
//		
//		rec.clear();
//		rec.put("name", new String("zzz"));
//		rec.put("id", new Integer(6));
//		rec.put("gpa", 1);
//		rec.put("Date of Birth", new Date(2010-1900,10-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//		
//		rec.clear();
//		rec.put("name", new String("zzz"));
//		rec.put("id", new Integer(7));
//		rec.put("gpa", 700);
//		rec.put("Date of Birth", new Date(2010-1900,10-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//		for (int i = 8; i < 17; i++) {
//			rec = new Hashtable();
//			rec.put("id", new Integer(i));
//			rec.put("name", new String("A"));
//			rec.put("gpa", 1);
//			rec.put("Date of Birth", new Date(2015 - 1900, 9 - 1, 17));
//			dbApp.insertIntoTable("Student", rec);
//		}

		
//		
//		rec.clear();
//		dbApp.getPages("Student");

//		rec.put("id", new Integer(1));
//		rec.put("name", new String("fafa"));
//		rec.put("Date of Birth", new Date(1990-1900,12-1,26));
//			dbApp.insertIntoTable("Student", rec);
			
//			
//			dbApp.getPages("Student");
		

		dbApp.getPages("Student");
		

		
		
	}

 

	private static void creating(String strTableName,DBApp dbApp) throws DBAppException {

		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("x", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("Date of Birth", "java.util.Date");
		htblColNameType.put("gpa", "java.lang.Integer");
		htblColNameType.put("phone", "java.lang.Integer");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("x", "0");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("Date of Birth", "1999-01-01");
		htblColNameMin.put("gpa", "0");
		htblColNameMin.put("phone", "0");

		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "100");
		htblColNameMax.put("x", "10");
		htblColNameMax.put("name", "F");
		htblColNameMax.put("Date of Birth", "2050-12-31");
		htblColNameMax.put("gpa", "20");
		htblColNameMax.put("phone", "40");

		//dbApp.init();
		
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

	
	}
	public static void updating (String strTableName,DBApp dbApp) throws DBAppException {
		Hashtable rec = new Hashtable();

		rec.put("gpa", 4.0);
		rec.put("name", "hh");
//		rec.put("id", 919);
		dbApp.updateTable("Student","7", rec);
		dbApp.getPages("Student");
	}

}
