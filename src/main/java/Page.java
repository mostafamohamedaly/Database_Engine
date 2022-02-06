import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable {

	String tableName;
	Object minKey;
	Object maxKey;
	String clustringKey;
	int pageNum;
	int currentRowNumber;
	int maximumRowsCountinPage;
	Vector<Record> records;

	public Page(String tableName, String clustringKey, Integer maximumRowsCountinPage,Integer pageNum,Vector<Record> records) {
		this.tableName = tableName;
		this.clustringKey = clustringKey;
		this.maximumRowsCountinPage = maximumRowsCountinPage;
		this.pageNum = pageNum;
		this.records = records;
		this.updateMinMaxCurrent();
	}
	
	public Page(String tableName, Hashtable<String, Object> firstRecord, String clustringKey, int pageNum)
			throws DBAppException {
		Record record = new Record(this.tableName, firstRecord, clustringKey);
		this.tableName = tableName;
		this.clustringKey = clustringKey;
		this.maximumRowsCountinPage = readConfig();
		records = new Vector<Record>();
		this.records.add(record);
		this.minKey = records.get(0).getClustringKey();
		this.maxKey = records.get(0).getClustringKey();
		this.currentRowNumber = records.size();
		this.pageNum = pageNum;
	}

	public int getPageNum() {
		return pageNum;
	}

	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}

	public static int readConfig() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
			System.out.println("Config file not found!");
		}
		try {
			prop.load(is);
		} catch (IOException ex) {
		}
		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int binarySearchInsertion(Vector<Object> arr, Object x) {
		int l = 0, r = arr.size() - 1;
		// int m = 0;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (((Comparable)x).equals(arr.get(m)))
				return m;

			// If x greater, ignore left half
			if (((Comparable)x).compareTo(arr.get(m)) > 0)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was not present
		return l;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int binarySearch(Vector<Object> arr, Object x) {
		int l = 0, r = arr.size() - 1;
		// int m = 0;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (((Comparable)x).equals(arr.get(m)))
				return m;

			// If x greater, ignore left half
			if (((Comparable)x).compareTo(arr.get(m)) > 0)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was not present
		return -1;
	}

	public void add(Hashtable<String, Object> record) throws DBAppException {
		Record r = new Record(this.tableName, record, this.clustringKey);

		if (binarySearch(this.getClustringKeys(), r.getClustringKey()) == -1) {
			int index = binarySearchInsertion(this.getClustringKeys(), r.getClustringKey());
			this.records.insertElementAt(r, index);
			this.updateMinMaxCurrent();

		} else {
			throw new DBAppException("The clustring Key Already exists!");
		}
	}

	public Vector<Object> getClustringKeys() {
		Vector<Object> r = new Vector<Object>();
		for (Record element : records) {
			r.add(element.getClustringKey());
		}
		return r;
	}

	public boolean isEmpty() {
		if (this.currentRowNumber == 0)
			return true;
		return false;
	}

	public boolean isFull() {
		if (this.currentRowNumber == this.maximumRowsCountinPage)
			return true;
		return false;
	}

	public void updateMinMaxCurrent() {
		this.minKey = this.records.get(0).getClustringKey();
		this.maxKey = this.records.get(this.records.size() - 1).getClustringKey();
		this.currentRowNumber = this.records.size();
	}

	public void write() {
		String fileName = "src/main/resources/data/" + this.getTableName() + "Page" + pageNum + ".bin";
		try {
			FileOutputStream fileOs = new FileOutputStream(fileName);
			ObjectOutputStream os = new ObjectOutputStream(fileOs);
			os.writeObject(this.records);
			fileOs.close();
			os.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Hashtable<String, Object> getMinMaxCountN() {
		Hashtable<String, Object> v = new Hashtable<String, Object>();
		v.put("min", this.minKey);
		v.put("max", this.maxKey);
		v.put("count", this.currentRowNumber);
		v.put("pageN", this.pageNum);
		return v;
	}

	@Override
	public String toString() {
		return "Page [tableName=" + tableName + ", minKey=" + minKey + ", maxKey=" + maxKey + ", clustringKey="
				+ clustringKey + ", currentRowNumber=" + currentRowNumber + ", maximumRowsCountinPage="
				+ maximumRowsCountinPage + ", records=" + records + "]";
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Object getMinKey() {
		return minKey;
	}

	public void setMinKey(Object minKey) {
		this.minKey = minKey;
	}

	public Object getMaxKey() {
		return maxKey;
	}

	public void setMaxKey(Object maxKey) {
		this.maxKey = maxKey;
	}

	public String getClustringKey() {
		return clustringKey;
	}

	public void setClustringKey(String clustringKey) {
		this.clustringKey = clustringKey;
	}

	public int getCurrentRowNumber() {
		return currentRowNumber;
	}

	public void setCurrentRowNumber(int currentRowNumber) {
		this.currentRowNumber = currentRowNumber;
	}

	public int getMaximumRowsCountinPage() {
		return maximumRowsCountinPage;
	}

	public void setMaximumRowsCountinPage(int maximumRowsCountinPage) {
		this.maximumRowsCountinPage = maximumRowsCountinPage;
	}

	public Vector<Record> getRecords() {
		return records;
	}

	public void setRecords(Vector<Record> records) {
		this.records = records;
	}

}
