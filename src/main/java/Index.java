import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Index implements Serializable {

	int d;
	String tableName;
	Integer indexNum;
	Vector<String> columnNames;
	Vector<Vector<Hashtable<String, Object>>> grid;

	public Index(String tableName, String[] columnNames) throws DBAppException {
		this.tableName = tableName;
		this.grid = new Vector<Vector<Hashtable<String, Object>>>(columnNames.length);
		this.d = columnNames.length;
		this.columnNames = new Vector<String>(columnNames.length);
		Collections.addAll(this.columnNames, columnNames);
		for (String column : columnNames) {
			Vector<Hashtable<String, Object>> v = null;
			try {
				v = divideColumn(tableName, column);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			grid.add(v);
		}
		//this.createBuckets();
	}

	public void createBuckets() {
		Table table = DBApp.readTable(this.tableName);
		Vector<Hashtable<String, Object>> pageList = table.getMinMaxCountN();
		Vector<Hashtable<String, Object>> bucketList = this.createBucketList();
		Vector<Bucket> buckets = new Vector<>();
		buckets.setSize(bucketList.size());
		Collections.fill(buckets, null);
		for (Hashtable<String, Object> pageInfo : pageList) {
			Integer pageNum = (Integer) pageInfo.get("pageN");
			Page page = DBApp.readPage(this.tableName, pageNum, table.getClustringKey(),table.getMaxCount());
			for (Record record : page.getRecords()) {
				Hashtable<Integer, Vector<Object>> indicies = new Hashtable<Integer, Vector<Object>>();
				for (Hashtable<String, Object> bucketInfo : bucketList) {
					Vector<Integer> cell = (Vector<Integer>) bucketInfo.get("cell");
					String temp = "";
					for (Integer n : cell) {
						temp = temp + n;
					}
					Integer bucketNum = Integer.parseInt(temp);
					Vector<Hashtable<String, Object>> constraints = (Vector<Hashtable<String, Object>>) bucketInfo.get("range");
					Integer j = 0;
					for (int i = 0; i < this.columnNames.size(); i++) {
						Comparable min = (Comparable) constraints.get(i).get("min");
						Comparable max = (Comparable) constraints.get(i).get("max");
						Comparable value = (Comparable) record.getColNameValue().get(columnNames.get(i));
						if ((min instanceof Integer && value instanceof Integer)
								|| (min instanceof Double && value instanceof Double)
								|| (min instanceof String && value instanceof String)
								|| (min instanceof Date && value instanceof Date)) {
							if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0) {
								j++;
							}
						}
					}
					if (j == columnNames.size()) {
						try {
							Bucket bucket = DBApp.readBucket(this.tableName, this.indexNum, bucketNum);
							if(bucket.getIndicies().containsKey(pageNum)) {
								bucket.getIndicies().get(pageNum).add(record.getClustringKey());
							}
							else {
								Vector<Object> v = new Vector<>();
								v.add(record.getClustringKey());
								bucket.getIndicies().put(pageNum, v);
								buckets.set(bucketNum,bucket);
							}
						} catch (ClassNotFoundException | IOException e) {
							Bucket bucket = new Bucket(this.tableName,this.indexNum,this.columnNames,cell,constraints);
							Vector<Object> v = new Vector<>();
							v.add(record.getClustringKey());
							bucket.getIndicies().put(pageNum, v);
							buckets.set(bucketNum,bucket);
						}
					}
				}
			}
		}
		for(Bucket bucket: buckets) {
			if(bucket != null) {
				bucket.write();
			}
		}
	}

	// Search
	@SuppressWarnings("unchecked")
	public Integer Search(Hashtable<String, Object> colNameValue) {
		return Integer.parseInt(searchHelper(colNameValue, (Vector<String>) this.columnNames.clone(),
				(Vector<Vector<Hashtable<String, Object>>>) this.grid.clone()));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static String searchHelper(Hashtable<String, Object> colNameValue, Vector<String> colNames,
			Vector<Vector<Hashtable<String, Object>>> grid) {
		// Comparable x = new Integer(6);
		if (colNames.isEmpty())
			return "";
		for (int i = 0; i < grid.get(0).size(); i++) {
			Hashtable<String, Object> range = grid.get(0).get(i);
			String colName = colNames.get(0);
			Comparable value = (Comparable) colNameValue.get(colName);
			Comparable min = (Comparable) range.get("min");
			Comparable max = (Comparable) range.get("max");
			if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0) {
				colNames.remove(0);
				grid.remove(0);
				return i + searchHelper(colNameValue, colNames, grid);
			}

		}
		return null;
	}
	//

	public static Vector<Hashtable<String, Object>> divideColumn(String tableName, String columnName)
			throws IOException, DBAppException {
		Hashtable<String, Vector<String>> csv = DBApp.readCSV(tableName);
		Vector<String> meta = csv.get(columnName);
		Vector<Object> minMax = getMinMax(meta);
		Object min = minMax.get(0);
		Object max = minMax.get(1);
		Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
		if (min instanceof Integer) {
			int n = (((int) max - (int) min + 1) % 2);
			int div = ((int) max - (int) min) / 10;
			int d = (int) min;
			for (int i = 0; i < 9; i++) {
				Hashtable<String, Object> h = new Hashtable<String, Object>();
				h.put("min", d);
				if (n == 0)
					d += div;
				else
					d += div - 1;
				h.put("max", d);
				r.add(h);
				d++;
			}
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			h.put("min", d);
			h.put("max", max);
			r.add(h);
			return r;
		} else if (min instanceof Double) {
			Double div = ((Double) max - (Double) min) / 10;
			Double d = (Double) min;
			for (int i = 0; i < 9; i++) {
				Hashtable<String, Object> h = new Hashtable<String, Object>();
				h.put("min", d);
				d += div;
				h.put("max", d);
				r.add(h);
				d += 0.01;
			}
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			h.put("min", d);
			h.put("max", max);
			r.add(h);
			return r;
		} else if (min instanceof String) {
			return divideStr((String) min, (String) max);
		} else if (min instanceof Date) {
			Calendar startCalendar = Calendar.getInstance();
			startCalendar.clear();
			startCalendar.setTime((Date) min);
			Calendar endCalendar = Calendar.getInstance();
			endCalendar.clear();
			endCalendar.setTime((Date) max);
			int n = elapsed(startCalendar, endCalendar, Calendar.DATE);
			int div = n / 10;
			for (int i = 0; i < 9; i++) {
				Hashtable<String, Object> h = new Hashtable<String, Object>();
				Date d = startCalendar.getTime();
				startCalendar.add(Calendar.DATE, div);
				if (n == 0)
					startCalendar.add(Calendar.DATE, div);
				else
					startCalendar.add(Calendar.DATE, div - 1);
				Date auxDate = startCalendar.getTime();
				h.put("max", auxDate);
				h.put("min", d);
				r.add(h);
				startCalendar.add(Calendar.DATE, 1);
			}
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			Date sd = startCalendar.getTime();
			h.put("max", max);
			h.put("min", sd);
			r.add(h);
			return r;

		}
		return null;
	}

	private static int elapsed(Calendar before, Calendar after, int field) {

		Calendar clone = (Calendar) before.clone(); // Otherwise changes are been reflected.
		int elapsed = -1;
		while (!clone.after(after)) {
			clone.add(field, 1);
			elapsed++;
		}
		return elapsed;
	}

	@SuppressWarnings("deprecation")
	public static Vector<Object> getMinMax(Vector<String> c) {
		String t = c.get(0);
		Vector<Object> minMax = new Vector<Object>();

		if (t.equals("java.lang.Integer")) {
			Integer min = new Integer(c.get(3));
			Integer max = new Integer(c.get(4));
			minMax.add(min);
			minMax.add(max);
			return minMax;

		} else if (t.equals("java.lang.Double")) {
			Double min = new Double(c.get(3));
			Double max = new Double(c.get(4));
			minMax.add(min);
			minMax.add(max);
			return minMax;
		} else if (t.equals("java.lang.String")) {
			String min = c.get(3);
			String max = c.get(4);
			minMax.add(min);
			minMax.add(max);
			return minMax;

		} else if (t.equals("java.util.Date")) {

			Date min = null;
			try {
				min = new SimpleDateFormat("yyyy-MM-dd").parse(c.get(3));
			} catch (ParseException e1) {
			}
			Date max = null;
			try {
				max = new SimpleDateFormat("yyyy-MM-dd").parse(c.get(4));
			} catch (ParseException e) {
			}
			minMax.add(min);
			minMax.add(max);
			return minMax;
		}
		return minMax;

	}

	public static Vector<Hashtable<String, Object>> divideStr(String min, String max) {
		if (min.length() == max.length()) {
			return divideStrEqual(min, max, 10);
		} else {
			if ((max.length() - min.length()) + 1 >= 10) {
				return divideStrNotEqual(min, max);
			} else {

				Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
				Vector<Hashtable<String, Object>> v = divideStrNotEqual(min, max);
				int divisions = 10 / v.size();
				int t = 0;
				int n = divisions * v.size();
				for (int i = 0; i < v.size() - 1; i++) {
					Vector<Hashtable<String, Object>> r1 = divideStrEqual((String) v.get(i).get("min"),
							(String) v.get(i).get("max"), divisions);
					r.addAll(r1);
					t = divisions + t;
				}
				if (t < 10) {
					int o;
					if (n % t == 0)
						o = divisions;
					else
						o = divisions + 1;
					Vector<Hashtable<String, Object>> r1 = divideStrEqual((String) v.get(v.size() - 1).get("min"),
							(String) v.get(v.size() - 1).get("max"), o);
					r.addAll(r1);
				}
				return r;
			}
		}
	}

	public static Vector<Hashtable<String, Object>> divideStrNotEqual(String min, String max) {
		Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
		int n = 0;
		if ((max.length() - min.length()) + 1 > 10)
			n = 10;
		else
			n = (max.length() - min.length()) + 1;
		int minLength = min.length();
		char minChar = min.charAt(0);
		char maxChar = max.charAt(0);
		for (int i = 0; i < n - 1; i++) {
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			String minStr = new String(new char[minLength]).replace('\0', minChar);
			h.put("min", minStr);

			String maxStr = new String(new char[minLength]).replace('\0', maxChar);
			h.put("max", maxStr);
			r.add(h);
			minLength++;
		}
		Hashtable<String, Object> h = new Hashtable<String, Object>();
		String minStr = new String(new char[minLength]).replace('\0', minChar);
		h.put("min", minStr);
		String maxStr = new String(new char[max.length()]).replace('\0', maxChar);
		h.put("max", maxStr);
		r.add(h);
		return r;
	}

	public static Vector<Hashtable<String, Object>> divideStrEqual(String min, String max, int division) {
		// int size = min.length();
		Vector<Hashtable<String, Object>> r1 = new Vector<Hashtable<String, Object>>(division);
		for (int j = 0; j < division; j++) {
			Hashtable<String, Object> temp = new Hashtable<String, Object>();
			temp.put("min", min.substring(0, min.length() - 1));
			temp.put("max", max.substring(0, max.length() - 1));
			r1.add(temp);
		}
		int x = 0;
		Vector<Hashtable<String, String>> r2 = new Vector<Hashtable<String, String>>(division);
		int minCode = min.codePointAt(x);
		int maxCode = max.codePointAt(x);
		int n = (maxCode - minCode + 1) % 2;
		int div = (maxCode - minCode) / division;
		int d = minCode;
		for (int j = 0; j < division - 1; j++) {
			Hashtable<String, String> h = new Hashtable<String, String>();
			h.put("min", (char) d + "");
			if (n == 0)
				d += div;
			else
				d += div - 1;
			h.put("max", (char) d + "");
			r2.add(h);
			d++;
		}
		Hashtable<String, String> h = new Hashtable<String, String>();
		h.put("min", (char) d + "");
		h.put("max", (char) maxCode + "");
		r2.add(h);
		for (int j = 0; j < division; j++) {
			r1.get(j).replace("min", r2.get(j).get("min") + r1.get(j).get("min"));
			r1.get(j).replace("max", r2.get(j).get("max") + r1.get(j).get("max"));
		}
		// }

		return r1;
	}

	@SuppressWarnings("unchecked")
	public Vector<Hashtable<String, Object>> createBucketList() {
		Vector<Vector<Hashtable<String, Object>>> v = (Vector<Vector<Hashtable<String, Object>>>) (this.grid).clone();
		Vector<Hashtable<String, Object>> f = fact(v.remove(0));
		while (!v.isEmpty()) {
			f = multVector(f, v.remove(0));
		}
		return f;
	}

	public static Vector<Hashtable<String, Object>> fact(Vector<Hashtable<String, Object>> v) {
		Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
		for (int i = 0; i < v.size(); i++) {
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			Vector<Integer> cell = new Vector<Integer>();
			cell.add(i);
			h.put("cell", cell);
			Vector<Hashtable<String, Object>> g = new Vector<Hashtable<String, Object>>();
			g.add(v.get(i));
			h.put("range", g);
			r.add(h);
		}
		return r;
	}

	@SuppressWarnings("unchecked")
	public static Vector<Hashtable<String, Object>> multVector(Vector<Hashtable<String, Object>> f,
			Vector<Hashtable<String, Object>> l) {
		Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
		for (int i = 0; i < f.size(); i++) {
			for (int j = 0; j < l.size(); j++) {
				Vector<Integer> cell = (Vector<Integer>) ((Vector<Integer>) f.get(i).get("cell")).clone();
				Vector<Hashtable<String, Object>> range = (Vector<Hashtable<String, Object>>) ((Vector<Hashtable<String, Object>>) f
						.get(i).get("range")).clone();
				cell.add(j);
				range.add(l.get(j));
				Hashtable<String, Object> h = new Hashtable<String, Object>();
				h.put("cell", cell);
				h.put("range", range);
				r.add(h);
			}
		}
		return r;
	}

	public void write() {
		String fileName = "src/main/resources/data/index/" + this.tableName + "Index" + this.indexNum + ".bin";
		try {
			FileOutputStream fileOs = new FileOutputStream(fileName);
			@SuppressWarnings("resource")
			ObjectOutputStream os = new ObjectOutputStream(fileOs);
			os.writeObject(this);
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
	}

	@Override
	public String toString() {
		return "Index [d=" + d + ", tableName=" + tableName + ", indexNum=" + indexNum + ", columnNames=" + columnNames
				+ ", grid=" + grid + "]";
	}

	public int getD() {
		return d;
	}

	public void setD(int d) {
		this.d = d;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Vector<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(Vector<String> columnNames) {
		this.columnNames = columnNames;
	}

	public Vector<Vector<Hashtable<String, Object>>> getGrid() {
		return grid;
	}

	public void setGrid(Vector<Vector<Hashtable<String, Object>>> grid) {
		this.grid = grid;
	}

	public Integer getIndexNum() {
		return indexNum;
	}

	public void setIndexNum(Integer indexNum) {
		this.indexNum = indexNum;
	}

}
