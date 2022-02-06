import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Bucket implements Serializable {
	int d;
	String tableName;
	Integer indexNum;
	Integer cell;
	Vector<String> columnNames;
	Vector<Hashtable<String, Object>> constraints;
	Hashtable<Integer, Vector<Object>> indicies;

	@SuppressWarnings("unchecked")
	public Bucket(String tableName, Integer indexNum, Vector<String> columnNames, Vector<Integer> cell,
			Vector<Hashtable<String, Object>> constraints) {
		this.tableName = tableName;
		this.indexNum = indexNum;
		this.columnNames = columnNames;
		this.d = columnNames.size();
		String temp = "";
		for (Integer n : cell) {
			temp = temp + n;
		}
		this.cell = Integer.parseInt(temp);
		this.constraints = constraints;
		this.indicies = new Hashtable<Integer, Vector<Object>>();
		this.indicies = new Hashtable<>();
		//Table table = DBApp.readTable(tableName);
		//Vector<Hashtable<String, Object>> minMax = null;
		//int clustringIndex = 0;
		//if (columnNames.contains(table.clustringKey)) {
		//	clustringIndex = columnNames.indexOf(table.clustringKey);
		//	minMax = filterWithClustringKey((Vector<Hashtable<String, Object>>) table.getMinMaxCountN().clone(),
		//			constraints.get(clustringIndex));
		//} else {
		//	minMax = (Vector<Hashtable<String, Object>>) table.getMinMaxCountN().clone();
		//}
		//search(minMax, table.clustringKey);
	}
	/*
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void search(Vector<Hashtable<String, Object>> pageList, String clustringKey) {
		//System.out.println(pageList);
		for (Hashtable<String, Object> pageInfo : pageList) {
			Vector<Object> pageIndecies = new Vector<Object>();
			Object pageNum = pageInfo.get("pageN");
			Page page = DBApp.readPage(this.tableName, pageNum, table.getClustringKey(),table.getMaxCount());
			int start = 0;
			int end = page.records.size();
			Vector<String> colNames = (Vector<String>) this.columnNames.clone();
			Vector<Hashtable<String, Object>> con = (Vector<Hashtable<String, Object>>) this.constraints.clone();
			if (columnNames.contains(clustringKey)) {
				//System.out.println("Start");
				int clustringIndex = columnNames.indexOf(clustringKey);
				Object min = this.constraints.get(clustringIndex).get("min");
				Object max = this.constraints.get(clustringIndex).get("max");
				int minLoc = Page.binarySearch(page.getClustringKeys(), min);
				int maxLoc = Page.binarySearch(page.getClustringKeys(), max);
				if (minLoc >= 0) {
					start = minLoc;
				}
				if (maxLoc >= 0) {
					end = maxLoc;
				}
				colNames.remove(clustringIndex);
			}
			for (int i = start; i < end; i++) {
				//System.out.println("Start: "+start+" | End:"+end);
				int j = 0;
				Record record = page.records.get(i);
				//System.out.println(record);
				for (int k = 0; k < con.size(); k++) {
					Hashtable<String, Object> c = con.get(k);
					String colName = this.columnNames.get(k);
					//System.out.println("Here");
					Comparable min = (Comparable) c.get("min");
					Comparable max = (Comparable) c.get("max");
					Comparable value = (Comparable) record.getColNameValue().get(colName);
					if ((min instanceof Integer && value instanceof Integer)
							|| (min instanceof Double && value instanceof Double)
							|| (min instanceof String && value instanceof String)
							|| (min instanceof Date && value instanceof Date)) {
						if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0) {
							j++;
						}
					}
				}
				if (j == con.size()) {
					//System.out.println(record.getClustringKey());
					pageIndecies.add(record.getClustringKey());
				}
			}
			if(!pageIndecies.isEmpty())
				this.indicies.put((Integer) pageNum, pageIndecies);
		}
	}
	*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Vector<Hashtable<String, Object>> filterWithClustringKey(
			Vector<Hashtable<String, Object>> minMaxCount, Hashtable<String, Object> constraint) {
		Vector<Hashtable<String, Object>> r = new Vector<Hashtable<String, Object>>();
		Comparable min = (Comparable) constraint.get("min");
		Comparable max = (Comparable) constraint.get("max");
		for (Hashtable<String, Object> page : minMaxCount) {
			if (min.compareTo(page.get("max")) > 0) {
				continue;
			}
			if (max.compareTo(page.get("min")) < 0) {
				break;
			}
			if (max.compareTo(page.get("max")) <= 0) {
				r.add(page);
				break;
			} else {
				r.add(page);
				continue;
			}
		}
		return r;
	}

	public void write() {
		//System.out.println(this.indicies);
		Enumeration<Vector<Object>> pages = this.indicies.elements();
		int n = this.indicies.size();
		while (pages.hasMoreElements()) {
			Vector<Object> p = pages.nextElement();
			if(p.isEmpty()) {
				n--;
			}
		}
		if(n != 0) {
		String fileName = "src/main/resources/data/index/" + this.tableName + "Bucket" + this.indexNum + "-" + this.cell
				+ ".bin";
		try {
			FileOutputStream fileOs = new FileOutputStream(fileName);
			ObjectOutputStream os = new ObjectOutputStream(fileOs);
			os.writeObject(this);
			os.close();
			os.flush();
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
		}
		else {
			try {
				DBApp.readBucket(this.tableName, this.indexNum, this.cell);
				File b = new File("src/main/resources/data/index/" + this.tableName + "Bucket" + this.indexNum + "-" + this.cell
						+ ".bin");
				b.delete();
			} catch (ClassNotFoundException | IOException e) {
			}
			
		}
	}

	@Override
	public String toString() {
		return "Bucket [d=" + d + ", tableName=" + tableName + ", indexNum=" + indexNum + ", cell=" + cell
				+ ", columnNames=" + columnNames + ", constraints=" + constraints + ", indicies=" + indicies + "]";
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

	public Integer getIndexNum() {
		return indexNum;
	}

	public void setIndexNum(Integer indexNum) {
		this.indexNum = indexNum;
	}

	public Integer getCell() {
		return cell;
	}

	public void setCell(Integer cell) {
		this.cell = cell;
	}

	public Vector<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(Vector<String> columnNames) {
		this.columnNames = columnNames;
	}

	public Vector<Hashtable<String, Object>> getConstraints() {
		return constraints;
	}

	public void setConstraints(Vector<Hashtable<String, Object>> constraints) {
		this.constraints = constraints;
	}

	public Hashtable<Integer, Vector<Object>> getIndicies() {
		return indicies;
	}

	public void setIndicies(Hashtable<Integer, Vector<Object>> indicies) {
		this.indicies = indicies;
	}

}
