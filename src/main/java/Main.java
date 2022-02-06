import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.lang.Math;

public class Main {

	
	
	public static void printTable(String strTableName) {
		Table table = DBApp.readTable(strTableName);
		System.out.println("\nTable "+strTableName);
		System.out.println("\nPages");
		for(Hashtable<String,Object> p : table.getMinMaxCountN()) {
			System.out.println(DBApp.readPage(strTableName, p.get("pageN"), table.getClustringKey(),table.getMaxCount()));
		}
		try {
			Vector<Vector<String>> indexTable = DBApp.readIndexTable(strTableName);
			System.out.println("\nIndex Table");
			System.out.println(indexTable);
			for(int i = 0;i < indexTable.size();i++) {
				Index index = DBApp.readIndex(strTableName, i);
				System.out.println("\nIndex"+i);
				Integer bucketCount = (int) Math.pow(10,index.getD());
				for(int b = 0; b <= bucketCount;b++) {
					try {
					Bucket bucket = DBApp.readBucket(index.getTableName(), index.getIndexNum(), b);
					System.out.println(b + " "+bucket);
					}catch (ClassNotFoundException | IOException e) {
					}
				}
			}
				
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("\nIndex Table does not exist");
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws DBAppException {

		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "AAAAAA");
		htblColNameMin.put("gpa", "0.7");
		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "zzzzzz");
		htblColNameMax.put("gpa", "5.0");

		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();

		dbApp.createIndex(strTableName, new String[] { "gpa", "name" });
		
		htblColNameValue.put("id", new Integer(0));
		htblColNameValue.put("name", new String("AAAAAA"));
		htblColNameValue.put("gpa", new Double(0.9));
		dbApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();

		htblColNameValue.put("id", new Integer(2));
		htblColNameValue.put("name", new String("AAAAAA"));
		htblColNameValue.put("gpa", new Double(0.95));
		dbApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();

		htblColNameValue.put("id", new Integer(3));
		htblColNameValue.put("name", new String("ZiDDlx"));
		htblColNameValue.put("gpa", new Double(1.25));
		dbApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		
		//dbApp.createIndex(strTableName, new String[] { "gpa", "name" });
		
		htblColNameValue.put("id", new Integer(4));
		htblColNameValue.put("name", new String("ZiDDlx"));
		htblColNameValue.put("gpa", new Double(1.5));
		dbApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();

		htblColNameValue.put("id", new Integer(5));
		htblColNameValue.put("name", new String("kqQULY"));
		htblColNameValue.put("gpa", new Double(0.88));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(1));
		htblColNameValue.put("name", new String("jifhai"));
		htblColNameValue.put("gpa", new Double(1.1));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		dbApp.createIndex(strTableName, new String[] { "name" });
		
		htblColNameValue.clear();
		htblColNameValue.put("name", new String("kqQULY"));
		htblColNameValue.put("gpa", new Double(0.88));
		dbApp.deleteFromTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		//htblColNameValue.put("name", new String("kqQULY"));
		htblColNameValue.put("id", new Integer(6));
		//htblColNameValue.put("gpa", new Double(4));
		dbApp.updateTable(strTableName, "0" , htblColNameValue);
		
		printTable(strTableName);
		
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "ZiDDlx";
//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName= "gpa";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new Double( 1.25 );
		String[]strarrOperators = new String[0];
		//strarrOperators[0] = "AND";
		
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		
		while(resultSet.hasNext()) {
			System.out.println(resultSet.next());
		}
	}
}
