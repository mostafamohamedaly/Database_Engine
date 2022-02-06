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
public class Table implements Serializable {

	String tableName;
	String clustringKey;
	int maxCount;
	Hashtable<String, String> colNameType;
	Hashtable<String, String> colNameMin;
	Hashtable<String, String> colNameMax;
	Vector<Hashtable<String, Object>> minMaxCountN;
//Vector<Vector<Hashtable>> tableData;

	public Table(String tableName, String clustringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		super();
		this.tableName = tableName;
		this.clustringKey = clustringKey;
		this.maxCount = readConfig();
		this.colNameType = colNameType;
		this.colNameMin = colNameMin;
		this.colNameMax = colNameMax;
		// this.index = new Vector<Object>();
		this.minMaxCountN = new Vector<Hashtable<String, Object>>();

		// this.tableData = new Vector<Vector<Hashtable>>();
	}

	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public void add(Hashtable<String, Object> page) {
		int index = binarySearchPage(this.minMaxCountN, page);
		this.minMaxCountN.insertElementAt(page, index);
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

	public void write() {
		String fileName = "src/main/resources/data/" + this.tableName + ".bin";
		try {
			FileOutputStream fileOs = new FileOutputStream(fileName);
			@SuppressWarnings("resource")
			ObjectOutputStream os = new ObjectOutputStream(fileOs);
			os.writeObject(this);
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
	}

	public static int binarySearchPage(Vector<Hashtable<String, Object>> arr, Hashtable<String, Object> h) {
		int x = (int) h.get("pageN");
		int l = 0, r = arr.size() - 1;

		while (l <= r) {
			int m = l + (r - l) / 2;

			if ((int) arr.get(m).get("pageN") == x)
				return m;

			if ((int) arr.get(m).get("pageN") < x)
				l = m + 1;

			else
				r = m - 1;
		}

		return l;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getClustringKey() {
		return clustringKey;
	}

	public void setClustringKey(String clustringKey) {
		this.clustringKey = clustringKey;
	}

	public Hashtable<String, String> getColNameType() {
		return colNameType;
	}

	public void setColNameType(Hashtable<String, String> colNameType) {
		this.colNameType = colNameType;
	}

	public Hashtable<String, String> getColNameMin() {
		return colNameMin;
	}

	public void setColNameMin(Hashtable<String, String> colNameMin) {
		this.colNameMin = colNameMin;
	}

	public Hashtable<String, String> getColNameMax() {
		return colNameMax;
	}

	public void setColNameMax(Hashtable<String, String> colNameMax) {
		this.colNameMax = colNameMax;
	}

	public Vector<Hashtable<String, Object>> getMinMaxCountN() {
		return minMaxCountN;
	}

	public void setMinMaxCountN(Vector<Hashtable<String, Object>> minMaxCountN) {
		this.minMaxCountN = minMaxCountN;
	}

	public static void add(Hashtable<String, Object> page, Vector<Hashtable<String, Object>> minMaxCountN) {
		int index = binarySearchPage(minMaxCountN, page);
		minMaxCountN.insertElementAt(page, index);
	}

	@Override
	public String toString() {
		return "Table [tableName=" + tableName + ", clustringKey=" + clustringKey + ", maxCount=" + maxCount
				+ ", colNameType=" + colNameType + ", colNameMin=" + colNameMin + ", colNameMax=" + colNameMax
				+ ", minMaxCountN=" + minMaxCountN + "]";
	}
}