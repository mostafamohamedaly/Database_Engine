import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	//Core Methods
	@Override
	public void init() {
	}
	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

		if (colNameType.containsKey(clusteringKey)) {
			Table table = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

			File dir = new File("src/main/resources/data");
			String[] pages = dir.list();
			boolean f = false;
			if (pages.length == 0) {
				f = false;
			} else {
				for (int i = 0; i < pages.length; i++) {
					String pageName = pages[i];
					if (pageName.equals(tableName + ".bin")) {
						f = true;
						break;
					}
				}
			}

			if (f) {
				throw new DBAppException("The table already exists!");
			} else {
				writeCSV(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
				String fileName = "src/main/resources/data/" + tableName + ".bin";
				try {
					FileOutputStream fileOs = new FileOutputStream(fileName);
					ObjectOutputStream os = new ObjectOutputStream(fileOs);
					os.writeObject(table);
					fileOs.close();
					os.close();
				} catch (FileNotFoundException e) {

				} catch (IOException e) {

				}
			}

		} else {
			throw new DBAppException("The Clustering Key is not found!");
		}
	}
	@SuppressWarnings("unchecked")
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {

		Vector<Vector<String>> indexTable = null;
		File indexTableFile = new File("src/main/resources/data/index/" + tableName + "IndexTable.bin");
		try {
			FileInputStream fileIs = new FileInputStream(indexTableFile);
			ObjectInputStream oIs = new ObjectInputStream(fileIs);
			indexTable = (Vector<Vector<String>>) oIs.readObject();
			fileIs.close();
			oIs.close();
		} catch (FileNotFoundException e1) {
			indexTable = new Vector<Vector<String>>();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		}

		Index index = new Index(tableName, columnNames);
		indexTable.add(index.getColumnNames());
		int indexNum = indexTable.size() - 1;
		index.setIndexNum(indexNum);
		index.createBuckets();
		index.write();
		writeIndexTable(tableName, indexTable);
		//Vector<Hashtable<String, Object>> bucketList = index.createBucketList();
		//for (Hashtable<String, Object> bucketInfo : bucketList) {
		//	Bucket bucket = new Bucket(tableName, indexNum, index.getColumnNames(),
		//			(Vector<Integer>) bucketInfo.get("cell"),
		//			(Vector<Hashtable<String, Object>>) bucketInfo.get("range"));
		//	bucket.write();
		//}
		try {
			updateIndexCSV(tableName, index.getColumnNames());
		} catch (IOException e) {
		}
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

		Table table = null;
		File dir = new File("src/main/resources/data");
		String[] pages = dir.list();
		if (pages.length == 0) {
			throw new DBAppException("The database is empty!");
		}
		if (!contains(pages, tableName))
			throw new DBAppException("Table " + tableName + " does not exist in the database!");
		else {
		}
		table = readTable(tableName);
		if (compareMeta(table.getTableName(), colNameValue, table.getClustringKey()) == true) {
			//New
			Enumeration colNamesEnum = colNameValue.keys();
			Vector<String> colNames =  enumVector(colNamesEnum);
			Vector<Integer> indexNums = getIndexNumbers(tableName,(Vector<String>)colNames.clone());
			Vector<Index> indecies = new Vector<Index>();
			
			if(!indexNums.isEmpty()) {
				for(Integer indexNum: indexNums)
					indecies.add(readIndex(tableName,indexNum));
			}
			//end new
			String[] tablePages = filter(pages, tableName);
			if (tablePages.length == 0) {
				Page page = new Page(table.getTableName() , colNameValue , table.getClustringKey(), 0);
				table.add(page.getMinMaxCountN());
				page.write();
				table.write();
				//new
				if(!indecies.isEmpty()) {
					insertIntoBucket(tableName,page,indecies ,colNames,colNameValue);
				}
				//end new
			} else {
				String key = table.getClustringKey();
				Object k = colNameValue.get(key);
				for (Hashtable<String, Object> element : table.getMinMaxCountN()) {
					table = readTable(tableName);
						
						if ((((((Comparable) k).compareTo(element.get("min")) > 0))
								&& ((((Comparable) k).compareTo(element.get("max"))) < 0))
								|| ((((Comparable) k).compareTo(element.get("min")) < 0))) {
							if ((int) element.get("count") < table.getMaxCount()) {
								Page page = readPage(table.getTableName(), element.get("pageN"), table.getClustringKey(),table.getMaxCount());
								page.add(colNameValue);
								table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(),
										(int) element.get("pageN"));
								page.write();
								table.write();
								//new 
								if(!indecies.isEmpty()) {
									insertIntoBucket(tableName,page,indecies ,colNames,colNameValue);
								}
								//end new
								break;
							}
							
							else {
								
								if ((table.getMinMaxCountN().size() - 1) == (int) element.get("pageN")) {
									Page pagef = readPage(table.getTableName(), element.get("pageN"), table.getClustringKey(),table.getMaxCount());
									//new 
									Hashtable<String,Object> oldRecord = pagef.getRecords().remove(pagef.getRecords().size() - 1).getColNameValue();
									Object ck = oldRecord.get(table.getClustringKey());
									//new end
									Page pageS = new Page(table.getTableName(), oldRecord, table.getClustringKey(), ((int) element.get("pageN") + 1));
									pagef.add(colNameValue);
									table.getMinMaxCountN().setElementAt(pagef.getMinMaxCountN(),
											(int) element.get("pageN"));
									table.add(pageS.getMinMaxCountN());
									pageS.write();
									pagef.write();
									table.write();
									//new
									if(!indecies.isEmpty()) {
										insertIntoBucket(tableName,pagef,indecies ,colNames,colNameValue);
										moveFromBucket(tableName, ck ,pageS,indecies ,colNames,oldRecord);
									}
									//new end
									break;
								} 
								else if ((int) table.getMinMaxCountN().get((int) element.get("pageN") + 1)
										.get("count") < table.getMaxCount()) {
									Object p = (((int) element.get("pageN")) + 1);
									Page pagef = readPage(table.getTableName(), element.get("pageN"), table.getClustringKey(),table.getMaxCount());
									Page pageS = readPage(table.getTableName(), p, table.getClustringKey(),table.getMaxCount());
									//new
									Hashtable<String,Object> oldRecord = pagef.getRecords().remove(pagef.getRecords().size() - 1).getColNameValue();
									Object ck = oldRecord.get(table.getClustringKey());
									//end new
									pageS.add(oldRecord);
									pagef.add(colNameValue);
									table.getMinMaxCountN().setElementAt(pagef.getMinMaxCountN(),
											(int) element.get("pageN"));
									table.getMinMaxCountN().setElementAt(pageS.getMinMaxCountN(), (int) p);
									pageS.write();
									pagef.write();
									table.write();
									//new
									if(!indecies.isEmpty()) {
										insertIntoBucket(tableName,pagef,indecies ,colNames,colNameValue);
										moveFromBucket(tableName, ck ,pageS,indecies ,colNames,oldRecord);
									}
									//end new
									break;
								} 
								else {
									int i = (int) element.get("pageN");
									Hashtable<String, Object> x = colNameValue;
									while (true) {

										if (!(table.getMinMaxCountN().size() == i)
												&& (int) table.getMinMaxCountN().get(i).get("count") == table
														.getMaxCount()) {
											Page page = readPage(table.getTableName(), i, table.getClustringKey(),table.getMaxCount());
											Hashtable<String, Object> y = (page.getRecords()
													.remove(page.getRecords().size() - 1).getColNameValue());
											page.add(x);
											if(!indecies.isEmpty()) {
												System.out.print("9 ");
												insertIntoBucket(tableName,page,indecies ,colNames,x);
											}
											x = y;
											if(!indecies.isEmpty()) {
												removeFromBucket(tableName,page,indecies ,colNames,x);
											}
											table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(), i);
											// x = y;
											page.write();
										} else {
											if (table.getMinMaxCountN().size() == i) {
												Page page = new Page(table.getTableName(), x, table.getClustringKey(),
														i);
												// page.add(x);
												table.add(page.getMinMaxCountN());
												page.write();
												if(!indecies.isEmpty()) {
													insertIntoBucket(tableName,page,indecies ,colNames,x);
												}
												
											} else if ((int) table.getMinMaxCountN().get(i).get("count") < table
													.getMaxCount()) {
												Page page = readPage(table.getTableName(), i, table.getClustringKey(),table.getMaxCount());
												page.add(x);
												table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(), i);
												page.write();
												if(!indecies.isEmpty()) {
													System.out.print("6 ");
													insertIntoBucket(tableName,page,indecies ,colNames,x);
												}
											}
											
											break;
										}
										i++;
									}
									table.write();
									break;
								}
							}
							//
						} else if ((((Comparable) k).compareTo(element.get("min")) >= 0)) {
							if ((int) element.get("count") < table.getMaxCount()) {
								Page page = readPage(table.getTableName(), element.get("pageN"), table.getClustringKey(),table.getMaxCount());
								page.add(colNameValue);
								table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(),
										(int) element.get("pageN"));
								page.write();
								table.write();
								if(!indecies.isEmpty()) {
									insertIntoBucket(tableName,page,indecies ,colNames,colNameValue);
								}
								break;
							} else {
								if ((table.getMinMaxCountN().size() - 1) == (int) element.get("pageN")
										&& (((Comparable) k).compareTo(element.get("max"))) >= 0) {
									Page page = new Page(table.getTableName(), colNameValue, table.getClustringKey(),
											((int) element.get("pageN") + 1));
									table.add(page.getMinMaxCountN());
									page.write();
									table.write();
									if(!indecies.isEmpty()) {
										insertIntoBucket(tableName,page,indecies ,colNames,colNameValue);
									}
									break;
								}
							}

						}
					 
				}
			}
		} else {
			throw new DBAppException("Data entered is not compatible!");
		}
	}
	@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		Table table = readTable(tableName);
		if (compareMeta(tableName, columnNameValue, table.getClustringKey()) == true && clusteringKeyValue != "" && clusteringKeyValue != null && tableName != "" && tableName != null) {
			String t = "";
			Object k = null;
			try {
				t = readCSV(tableName).get(table.getClustringKey()).get(0);
			} catch (IOException | DBAppException e) {
			}
			if (t.equals("java.lang.Integer")) {
				k = new Integer(clusteringKeyValue);
			} else if (t.equals("java.lang.Double")) {
				k = new Double(clusteringKeyValue);
			} else if (t.equals("java.lang.String")) {
				k = new String(clusteringKeyValue);
			} else if (t.equals("java.util.Date")) {
				try {
					k = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
				} catch (ParseException e) {
				}
			}
			else {
				throw new DBAppException("Data entered is incompatible");
			}
			
			//New
			Enumeration colNamesEnum = columnNameValue.keys();
			Vector<String> colNames = enumVector(colNamesEnum);
			Vector<Integer> indexNums = getIndexNumbers(tableName,(Vector<String>)colNames.clone());
			Vector<Index> indecies = new Vector<Index>();
			if(!indexNums.isEmpty()) {
				for(Integer indexNum: indexNums)
					indecies.add(readIndex(tableName,indexNum));
			}
			//end new
			for (Hashtable<String, Object> element : table.getMinMaxCountN()) {
				table = readTable(tableName);
					if (((((Comparable) k).compareTo(element.get("min")) >= 0))
							&& ((((Comparable) k).compareTo(element.get("max"))) <= 0)) {
						Page page = readPage(tableName, element.get("pageN"), table.getClustringKey(),table.getMaxCount());
						int index = Page.binarySearch(page.getClustringKeys(), k);
						if (index != -1) {
							if (columnNameValue.containsKey(table.getClustringKey())) {
								Enumeration<String> x = columnNameValue.keys();
								Record r = page.getRecords().remove(index);
								page.updateMinMaxCurrent();
								page.write();
								//new
								if(!indecies.isEmpty()) {
									removeFromBucket(tableName,page,indecies ,enumVector(r.getColNameValue().keys()) , r.getColNameValue());
								}
								//end new
								
								while (x.hasMoreElements()) {
									String s = x.nextElement();
									Object value = columnNameValue.get(s);
									r.getColNameValue().replace(s, value);
								}
								insertIntoTable(table.getTableName(), r.getColNameValue());
								return;
							} else {
								Enumeration<String> x = columnNameValue.keys();
								Record record = page.getRecords().get(index);
								if(!indecies.isEmpty()) {
									
									//enumVector(r.getColNameValue().keys())
									removeFromBucket(tableName,page,indecies ,enumVector(record.getColNameValue().keys()) , record.getColNameValue());
								} 
								while (x.hasMoreElements()) {
									String s = x.nextElement();
									Object value = columnNameValue.get(s);
									record.getColNameValue().replace(s, value);
								}
								
								record = page.getRecords().get(index);
								//System.out.println(record);
								
								page.write();
								if(!indecies.isEmpty()) {
									insertIntoBucket(tableName,page,indecies ,enumVector(record.getColNameValue().keys()),record.getColNameValue());
								}
								return;
							}

						} else {
							throw new DBAppException("Row not found!");
						}
					}


			}

		} else
			throw new DBAppException("Data entered is incompatible");
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table table = readTable(tableName);
		if (compareMeta(tableName, columnNameValue, table.getClustringKey()) == true) {
			//new
			Enumeration colNamesEnum = columnNameValue.keys();
			Vector<String> colNames = enumVector(colNamesEnum);
			Vector<Integer> indexNums = getIndexNumbers(tableName,(Vector<String>)colNames.clone());
			Vector<Index> indecies = new Vector<Index>();
			if(!indexNums.isEmpty()) {
				for(Integer indexNum: indexNums)
					indecies.add(readIndex(tableName,indexNum));
			}
			
			try {
				Vector<Vector<String>> indexTable = readIndexTable(tableName);
				Index index = null;
				for(int i = 0;i<indexTable.size();i++) {
					if(indexTable.get(i).size() == colNames.size() && indexTable.get(i).containsAll(colNames)) {
						index = readIndex(tableName,i);
						break;
					}	
				}
				if(index != null) {
					Integer indexNum = index.getIndexNum();
					Integer bucketNum = index.Search(columnNameValue);
					Bucket bucket = null;
					try {
					 bucket = readBucket(tableName, indexNum, bucketNum);
					} catch (ClassNotFoundException | IOException e1) {
						throw new DBAppException("No records to delete");
					}
					Enumeration<Integer> pageEnum = bucket.getIndicies().keys();
					while(pageEnum.hasMoreElements()) {
						Integer pageNum = pageEnum.nextElement();
						Page page = DBApp.readPage(tableName, pageNum, table.getClustringKey(),table.getMaxCount());
						Vector<Object> recordList = (Vector<Object>) bucket.getIndicies().get(pageNum).clone();
						for(Object recordCk: recordList) {
							Integer recordNum = Page.binarySearch(page.getClustringKeys(), recordCk);
							Record r = page.records.get(recordNum);
							page.records.removeElementAt(recordNum);
							page.setCurrentRowNumber(page.getCurrentRowNumber() - 1);
							page.updateMinMaxCurrent();
							table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(), pageNum);
							//bucket.getIndicies().get(pageNum).remove(recordCk);
							//bucket.write();
							if(!indecies.isEmpty()) {
								//Vector<Index> temp = (Vector<Index>) indecies.clone();
								//System.out.println(index);
								//temp.remove(index);
								//System.out.println(temp);
								removeFromBucket(tableName,page, indecies ,enumVector(r.getColNameValue().keys()) , r.getColNameValue());
							}
						}
						page.write();
						if (page.getCurrentRowNumber() == 0) {
							File p = new File("src/main/resources/data/" + tableName + "Page" + pageNum + ".bin");
							try {
								Files.deleteIfExists(p.toPath());
							} catch (IOException e) {
							}
							table.getMinMaxCountN().remove((int)pageNum);
							int max = table.getMinMaxCountN().size();
							for(Index ind:indecies)
								decrementBucketPages(ind, pageNum);
							for (int i = pageNum; i < max; i++) {
								Page nPage = readPage(table.getTableName(), i + 1, table.getClustringKey(),table.getMaxCount());
								File p1 = new File(
										"src/main/resources/data/" + tableName + "Page" + nPage.getPageNum() + ".bin");
								p1.delete();
								nPage.setPageNum(nPage.getPageNum() - 1);
								table.getMinMaxCountN().setElementAt(nPage.getMinMaxCountN(), nPage.getPageNum());
								nPage.write();
							}
						}
					}
					
					return;
				}
			} catch (ClassNotFoundException | IOException e1) {
			}
			//end new
			int pageSize = table.getMinMaxCountN().size();
			for (int pageCount = 0; pageCount < pageSize; pageCount++) {
				Hashtable<String, Object> element = table.getMinMaxCountN().get(pageCount);
				Page page = readPage(table.getTableName(), element.get("pageN"), table.getClustringKey(),table.getMaxCount());
				int recordSize = page.getRecords().size();
				for (int recordCount = 0; recordCount < recordSize; recordCount++) {
					Enumeration<String> colNamesEnum2 = columnNameValue.keys();
					int j = 0;
					while (colNamesEnum2.hasMoreElements()) {
						String colName = colNamesEnum2.nextElement();
						
						Comparable value1 = (Comparable) columnNameValue.get(colName);
						Comparable value2 = (Comparable) page.getRecords().get(recordCount).getColNameValue().get(colName);
						if ((value1 instanceof Integer && value2 instanceof Integer)
								|| (value1 instanceof Double && value2 instanceof Double)
								|| (value1 instanceof String && value2 instanceof String)
								|| (value1 instanceof Date && value2 instanceof Date)) {
							if (value2.equals(value1)) {
								j++;
							}
						}
					}
					if (j == columnNameValue.size()) {
						Record r = page.records.get(recordCount);
						page.records.removeElementAt(recordCount);
						page.setCurrentRowNumber(page.getCurrentRowNumber() - 1);
						page.updateMinMaxCurrent();
						table.getMinMaxCountN().setElementAt(page.getMinMaxCountN(), pageCount);
						if(!indecies.isEmpty()) {
							removeFromBucket(tableName,page, indecies , enumVector(r.getColNameValue().keys()) , r.getColNameValue());
						}
						recordCount--;
						recordSize--;
						
					}
				}
				page.write();
				if (page.getCurrentRowNumber() == 0) {

					File p = new File("src/main/resources/data/" + tableName + "Page" + pageCount + ".bin");
					int pageNum = pageCount;
					try {
						Files.deleteIfExists(p.toPath());
					} catch (IOException e) {
					}
					table.getMinMaxCountN().remove(pageNum);
					int max = table.getMinMaxCountN().size();
					for(Index ind:indecies)
						decrementBucketPages(ind, pageNum);
					for (int i = pageNum; i < max; i++) {
						Page nPage = readPage(table.getTableName(), i + 1, table.getClustringKey(),table.getMaxCount());
						File p1 = new File(
								"src/main/resources/data/" + tableName + "Page" + nPage.getPageNum() + ".bin");
						p1.delete();
						pageCount--;
						pageSize--;
						nPage.setPageNum(nPage.getPageNum() - 1);
						table.getMinMaxCountN().setElementAt(nPage.getMinMaxCountN(), nPage.getPageNum());
						nPage.write();
					}
				}
			}

		} else {
			throw new DBAppException("");
		}
		table.write();
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		Integer opCount = 0;
		Integer arrOpCount = 0;
		String tableName = sqlTerms[0]._strTableName;
		Table table = DBApp.readTable(tableName);
		Vector<Hashtable<String,Object>> r = new Vector<>();
		Vector<String> selectCols = new Vector<>();
		for(SQLTerm sqlTerm: sqlTerms) {
			selectCols.add(sqlTerm._strColumnName);
			if(sqlTerm._strOperator.equals("="))
				opCount++;
		}
		for(String arrayOperator: arrayOperators) {
			if(arrayOperator.equals("AND"))
				arrOpCount++;
		}
		if(arrayOperators.length != 0 && opCount == sqlTerms.length && arrOpCount == arrayOperators.length) {
			try {
				Vector<Vector<String>> indexTable = DBApp.readIndexTable(tableName);
				Index index = null;
				for(int i = 0; i < indexTable.size();i++ ) {
					if(indexTable.get(i).containsAll(selectCols)) {
						index = DBApp.readIndex(tableName, i);
						break;
					}	
				}
				if(index != null) {
					Hashtable<String,Object> h = new Hashtable<>();
					for(SQLTerm sqlTerm: sqlTerms)
						h.put(sqlTerm._strColumnName, sqlTerm._objValue);
					Integer bucketNum = index.Search(h);
					Bucket bucket = DBApp.readBucket(tableName, index.getIndexNum(), bucketNum);
					Vector<Integer> pageNums =  enumVector(bucket.getIndicies().keys());
					
					for(Integer pageNum: pageNums) {
						Page page = DBApp.readPage(tableName, pageNum, table.getClustringKey(),table.getMaxCount());
						Vector<Object> recordCks = bucket.getIndicies().get(pageNum);
						for(Object ck : recordCks){
							Integer loc = Page.binarySearch(page.getClustringKeys(), ck);
							Record record  = page.getRecords().get(loc);
							int j = 0;
							for(String selectCol: selectCols) {
								if(h.get(selectCol).equals(record.getColNameValue().get(selectCol)))
									j++;
							}
							if(j == selectCols.size()) 
								r.add(record.getColNameValue());
						}
					}
					return r.iterator();
				}
			} catch (ClassNotFoundException | IOException e) {
			}
			
		}
			//Table table = DBApp.readTable(tableName);
			for(Hashtable<String,Object> pageInfo: table.getMinMaxCountN()) {
				Integer pageNum = (Integer) pageInfo.get("pageN");
				Page page = DBApp.readPage(tableName, pageNum, table.getClustringKey(),table.getMaxCount());
				for(Record record: page.getRecords()) {
					Vector<Integer> t = new Vector<>();
					for(SQLTerm sqlTerm: sqlTerms) {
						switch(sqlTerm._strOperator) {
							case "=": 
								if(record.getColNameValue().get(sqlTerm._strColumnName).equals(sqlTerm._objValue)) 
									t.add(1);
								else 
									t.add(0);
								break;
							case "!=": 
								if(!record.getColNameValue().get(sqlTerm._strColumnName).equals(sqlTerm._objValue)) 
									t.add(1);
								else 
									t.add(0);
								break;
							case "<=": 
								if(((Comparable)record.getColNameValue().get(sqlTerm._strColumnName)).compareTo((Comparable)sqlTerm._objValue) <= 0) 
									t.add(1);
								else 
									t.add(0);
								break;
							case "<": 
								if(((Comparable)record.getColNameValue().get(sqlTerm._strColumnName)).compareTo((Comparable)sqlTerm._objValue) < 0) 
									t.add(1);
								else 
									t.add(0);
								break;
							case ">=": 
								if(((Comparable)record.getColNameValue().get(sqlTerm._strColumnName)).compareTo((Comparable)sqlTerm._objValue) >= 0) 
									t.add(1);
								else 
									t.add(0);
								break;
							case ">": 
								if(((Comparable)record.getColNameValue().get(sqlTerm._strColumnName)).compareTo((Comparable)sqlTerm._objValue) > 0) 
									t.add(1);
								else 
									t.add(0);
								break;
							default:
								throw new DBAppException("Wrong Operator!");
						}
					}
					Integer c = t.get(0);
					for(int i = 1; i < t.size();i++) {
						switch(arrayOperators[i-1]) {
							case "AND":
								c = c & t.get(i);
								break;
							case "OR":
								c = c | t.get(i);
								break;
							case "XOR":
								c = c ^ t.get(i);
								break;
							default:
								throw new DBAppException("Wrong Operator!");
						}
					}
					if(c == 1) {
						r.add(record.getColNameValue());
					}
				}
			}
		
		
		return r.iterator();
	}
	//
	
	//Writes
	public static void writeCSV(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		Vector<Vector<String>> rows = new Vector<Vector<String>>();
		Enumeration<String> columnName = colNameType.keys();
		while (columnName.hasMoreElements()) {
			Vector<String> row = new Vector<String>();
			String s = columnName.nextElement();
			row.add(tableName);
			row.add(s);
			row.add(colNameType.get(s));
			if (s.equals(clusteringKey))
				row.add("True");
			else
				row.add("False");
			row.add("False");
			row.add(colNameMin.get(s));
			row.add(colNameMax.get(s));
			rows.add(row);
		}
		FileWriter csvWriter = null;
		try {
			csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
		} catch (IOException e) {
		}

		for (Vector<String> rowData : rows) {
			try {
				csvWriter.append(String.join(",", rowData));
			} catch (IOException e) {
			}
			try {
				csvWriter.append("\n");
			} catch (IOException e) {
			}
		}

		try {
			csvWriter.flush();
		} catch (IOException e) {
		}
		try {
			csvWriter.close();
		} catch (IOException e) {
		}

	}
	public static void writeIndexTable(String tableName, Vector<Vector<String>> indexTable) {
		String fileName = "src/main/resources/data/index/" + tableName + "IndexTable.bin";
		try {
			FileOutputStream fileOs = new FileOutputStream(fileName);
			@SuppressWarnings("resource")
			ObjectOutputStream os = new ObjectOutputStream(fileOs);
			os.writeObject(indexTable);
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
	}
	//
	
	//Reads
	public static Table readTable(String tableName) {
		Table table = null;
		File tablePage = new File("src/main/resources/data/" + tableName + ".bin");
		try {
			FileInputStream fileIs = new FileInputStream(tablePage);
			ObjectInputStream oIs = new ObjectInputStream(fileIs);
			table = (Table) oIs.readObject();
			fileIs.close();
			oIs.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		}
		return table;
	}
	public static Page readPage(String tableName, Object pageNum , String clustringKey ,Integer maxCount ) {
		int p = (int) pageNum;
		File tablePage = new File("src/main/resources/data/" + tableName + "Page" + p + ".bin");
		Page page = null;
		try {
			FileInputStream fileIs = new FileInputStream(tablePage);
			ObjectInputStream oIs = new ObjectInputStream(fileIs);
			Vector<Record> records = (Vector<Record>) oIs.readObject();
			fileIs.close();
			oIs.close();
			page = new Page(tableName,clustringKey,maxCount,(Integer) pageNum,records);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		}

		return page;
	}
	public static Hashtable<String, Vector<String>> readCSV(String tableName1) throws IOException, DBAppException {
		Hashtable<String, Vector<String>> rows = new Hashtable<String, Vector<String>>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = br.readLine();
		while (current != null) {
			Vector<String> row = new Vector<String>();
			String[] line = current.split(",");
			String tableName = line[0];
			// row.add(tableName);
			if (tableName.equals(tableName1)) {
				String columnName = line[1];
				// row.add(columnName);
				String columnType = line[2];
				row.add(columnType);
				String ck = line[3];
				row.add(ck);
				String ind = line[4];
				row.add(ind);
				String min = line[5];
				row.add(min);
				String max = line[6];
				row.add(max);

				rows.put(columnName, row);
			}

			current = br.readLine();
		}
		br.close();

		return rows;
	}
	@SuppressWarnings("unchecked")
	public static Vector<Vector<String>> readIndexTable(String tableName) throws IOException, ClassNotFoundException{
		Vector<Vector<String>> indexTable = null;
		File indexTableFile = new File("src/main/resources/data/index/" + tableName + "IndexTable.bin");
		
		FileInputStream fileIs = new FileInputStream(indexTableFile);
		ObjectInputStream oIs = new ObjectInputStream(fileIs);
		indexTable = (Vector<Vector<String>>) oIs.readObject();
		fileIs.close();
		oIs.close();

		return indexTable;
	}
	public static Index readIndex(String tableName, Integer indexNum) {
		Index index = null;
		File indexFile = new File("src/main/resources/data/index/" + tableName + "Index" + indexNum +".bin");
		try {
			FileInputStream fileIs = new FileInputStream(indexFile);
			ObjectInputStream oIs = new ObjectInputStream(fileIs);
			index = (Index) oIs.readObject();
			fileIs.close();
			oIs.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		}
		return index;
	}
	public static Bucket readBucket(String tableName,Integer indexNum,Integer bucketNum) throws IOException, ClassNotFoundException {
		Bucket bucket = null;
		File bucketFile = new File("src/main/resources/data/index/" + tableName + "Bucket" + indexNum +"-" + bucketNum + ".bin");
		
			FileInputStream fileIs = new FileInputStream(bucketFile);
			ObjectInputStream oIs = new ObjectInputStream(fileIs);
			bucket = (Bucket) oIs.readObject();
			fileIs.close();
			oIs.close();
		return bucket;
	}
	//
	
	//Buckets
	@SuppressWarnings("unchecked")
	public void insertIntoBucket(String tableName,Page page,Vector<Index> indecies , Vector<String> colNames,Hashtable<String,Object> colNameValue) {
		
		for(Index index: indecies) {
			
			Vector<Vector<String>> colNamesPow = DBApp.powerSet(colNames);
			for(Vector<String> col: colNamesPow) {
				
				if(index.getColumnNames().size() == col.size() && index.getColumnNames().containsAll(col)) {
					
					Hashtable<String,Object> h = new Hashtable<String,Object>();
					for(Object s:col) {
						h.put((String) s, colNameValue.get(s));
					}
					Integer cell = index.Search(h);
					Bucket bucket = null;
					try {
						bucket = DBApp.readBucket(tableName,index.getIndexNum(),cell);
					} catch (ClassNotFoundException | IOException e) {
						Hashtable<String, Object> bucketInfo = index.createBucketList().get(cell);
						bucket = new Bucket(tableName,index.getIndexNum(),index.getColumnNames(),
								(Vector<Integer>)bucketInfo.get("cell"),(Vector<Hashtable<String, Object>>)bucketInfo.get("range"));
						//bucket.write();
						//return;
					}
					if(bucket.getIndicies().containsKey(page.pageNum)) {
						bucket.getIndicies().get(page.pageNum).add(colNameValue.get(page.getClustringKey()));
					}
					else {
						Vector<Object> b = new Vector<Object>();
						b.add(colNameValue.get(page.getClustringKey()));
						bucket.getIndicies().put(page.getPageNum(), b);
					}
					bucket.write();
				}
			}
		}
		
	}
	@SuppressWarnings("unchecked")
	public void moveFromBucket(String tableName, Object ck ,Page page,Vector<Index> indecies , Vector<String> colNames,Hashtable<String,Object> colNameValue) {
		for(Index index: indecies) {
			Vector<Vector<String>> colNamesPow = DBApp.powerSet(colNames);
			for(Vector<String> col: colNamesPow) {
				if(index.getColumnNames().size() == col.size() && index.getColumnNames().containsAll(col)) {
					Hashtable<String,Object> h = new Hashtable<String,Object>();
					for(Object s:col) {
						h.put((String) s, colNameValue.get(s));
					}
					Integer cell = index.Search(h);
					Bucket bucket = null;
					try {
						bucket = DBApp.readBucket(tableName,index.getIndexNum(),cell);
					} catch (ClassNotFoundException | IOException e) {
						Hashtable<String, Object> bucketInfo = index.createBucketList().get(cell);
						bucket = new Bucket(tableName,index.getIndexNum(),index.getColumnNames(),
								(Vector<Integer>)bucketInfo.get("cell"),(Vector<Hashtable<String, Object>>)bucketInfo.get("range"));
					}
					if(bucket.getIndicies().containsKey(page.pageNum)) {
						bucket.getIndicies().get(page.pageNum-1).remove(ck);
						bucket.getIndicies().get(page.pageNum).add(ck);
					}
					else {
						Vector<Object> b = new Vector<Object>();
						bucket.getIndicies().get(page.pageNum-1).remove(ck);
						b.add(colNameValue.get(page.getClustringKey()));
						bucket.getIndicies().put(page.getPageNum(), b);
					}
					bucket.write();
				}
			}
		}
		
	}
	public void removeFromBucket(String tableName,Page page,Vector<Index> indecies , Vector<String> colNames,Hashtable<String,Object> colNameValue) {
		for(Index index: indecies) {
			Vector<Vector<String>> colNamesPow = DBApp.powerSet(colNames);
			for(Vector<String> col: colNamesPow) {
				if(index.getColumnNames().size() == col.size() && index.getColumnNames().containsAll(col)) {
					Hashtable<String,Object> h = new Hashtable<String,Object>();
					for(Object s:col) {
						h.put((String) s, colNameValue.get(s));
					}
					Integer cell = index.Search(h);
					Bucket bucket = null;
					try {
						bucket = DBApp.readBucket(tableName,index.getIndexNum(),cell);
					} catch (ClassNotFoundException | IOException e) {
						return;
					}
					if(bucket.getIndicies().containsKey(page.pageNum)) {
						bucket.getIndicies().get(page.pageNum).remove(colNameValue.get(page.getClustringKey()));
					}
					bucket.write();
				}
			}
		}
		
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void decrementBucketPages(Index index, Integer startPage) {
		Integer bucketCount = (int) Math.pow(10,index.getD());
		for(int i = 0; i <= bucketCount;i++) {
			try {
				Bucket bucket = DBApp.readBucket(index.getTableName(), index.getIndexNum(), bucketCount);
				Enumeration bucketPageEnum = bucket.getIndicies().keys();
				Vector<Integer> bucketPages = DBApp.enumVector(bucketPageEnum);
				for(Integer bucketPageNum:bucketPages) {
					if(bucketPageNum == startPage) {
						bucket.getIndicies().remove(startPage);
					}
					else if(bucketPageNum > startPage) {
						Vector<Object> temp = bucket.getIndicies().remove(bucketPageNum);
						bucket.getIndicies().put(bucketPageNum-1,temp);
					}
				}
				bucket.write();
			} catch (ClassNotFoundException | IOException e) {
			}
		}
		
	}
	//
	
	//Other
	public static void updateIndexCSV(String tableName, Vector<String> columnNames)
			throws IOException/* , DBAppException */ {
		// Hashtable<String, Vector<String>> rows = new Hashtable<String,
		// Vector<String>>();
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = br.readLine();
		Vector<String[]> rows = new Vector<String[]>();
		while (current != null) {
			String[] line = current.split(",");
			if (line[0] == tableName) {
				for (String columnName : columnNames) {
					if (line[1] == columnName) {
						line[4] = "True";
						break;
					}
				}
			}
			current = br.readLine();
		}
		FileWriter csvWriter = null;
		try {
			csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
		} catch (IOException e) {
		}

		for (String[] rowData : rows) {
			try {
				csvWriter.append(String.join(",", rowData));
			} catch (IOException e) {
			}
			try {
				csvWriter.append("\n");
			} catch (IOException e) {
			}
		}
		try {
			csvWriter.flush();
		} catch (IOException e) {
		}
		try {
			csvWriter.close();
		} catch (IOException e) {
		}
		br.close();
	}
	@SuppressWarnings("deprecation")
	public static boolean compareMeta(String tableName, Hashtable<String, Object> colNameValue, String clustringKey) {
		// boolean f = true;
		Hashtable<String, Vector<String>> meta = null;

		try {
			meta = readCSV(tableName);
		} catch (IOException | DBAppException e) {
		}

		Enumeration<String> columnName = colNameValue.keys();
		while (columnName.hasMoreElements()) {
			String s = columnName.nextElement();
			if (!meta.containsKey(s)) {
				return false;
			}
			Vector<String> c = meta.get(s);

			String t = c.get(0);

			Object k = colNameValue.get(s);
			if (t.equals("java.lang.Integer")) {
				Integer min = new Integer(c.get(3));
				Integer max = new Integer(c.get(4));
				if (((int) k >= min) && ((int) k <= max)) {

				} else {
					return false;
				}
			} else if (t.equals("java.lang.Double")) {
				Double min = new Double(c.get(3));
				Double max = new Double(c.get(4));
				if (((((Double) k).compareTo(min) >= 0)) && ((((Double) k).compareTo(max)) <= 0)) {

				} else {
					return false;
				}
			} else if (t.equals("java.lang.String")) {
				String min = c.get(3);
				String max = c.get(4);
				if (((((String) k).compareTo(min) >= 0)) && ((((String) k).compareTo(max)) <= 0)) {

				} else {
					return false;
				}

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
				if (((((Date) k).compareTo(min) >= 0)) && ((((Date) k).compareTo(max)) <= 0)) {
					;
				} else {
					return false;
				}
			} else {
				return false;
			}

		}
		return true;

	}
	public static <E> Vector<E> enumVector(Enumeration<E> colNamesEnum){
		Vector<E> v = new Vector<E>();
		while(colNamesEnum.hasMoreElements()) {
			v.add(colNamesEnum.nextElement());
		}
		return v;
	}
	public static Vector<Integer> getIndexNumbers(String tableName,Vector<String> colNames){
		Vector<Vector<String>> indexTable;
		Vector<Integer> r = new Vector<Integer>();
		try {
			indexTable = readIndexTable(tableName);
		} catch (IOException | ClassNotFoundException e) {
			return r;
		}
		
		Vector<Vector<String>> colNamesPow = powerSet(colNames);
		for(int i = 0; i < indexTable.size(); i++) {
			for(Vector<String> c:colNamesPow) {
				if(indexTable.get(i).containsAll(c)) {
					if(!r.contains(i)) {
						r.add(i);
					}
				}
			}
		}
		return r;
	}
	public static <E> Vector<Vector<E>> powerSet(Vector<E> originalSet) {
	    Vector<Vector<E>> sets = new Vector<Vector<E>>();
	    if (originalSet.isEmpty()) {
	    	sets.add(new Vector<E>());
	        return sets;
	    }
	    Vector<E> list = new Vector<E>(originalSet);
	    E head = list.get(0);
	    Vector<E> rest = new Vector<E>(list.subList(1, list.size())); 
	    for (Vector<E> set : powerSet(rest)) {
	    	Vector<E> newSet = new Vector<E>();
	        newSet.add(head);
	        newSet.addAll(set);
	        sets.add(newSet);
	        sets.add(set);
	    }       
	    return sets;
	}
	//
	
	//String[] Manipulation
	public static boolean contains(Object[] arrObj, Object o) {
		boolean f = false;
		for (int i = 0; i < arrObj.length; i++) {
			if (arrObj[i].equals(o + ".bin")) {
				f = true;
				break;
			}
		}
		return f;
	}
	public static String[] filter(String[] arrStr, String str) {
		Vector<String> r = new Vector<String>();
		for (int i = 0; i < arrStr.length; i++) {
			if (arrStr[i].contains(str)) {
				if (!(arrStr[i].equals(str + ".bin")))
					r.add(arrStr[i]);
			} else {

			}
		}
		return vectorArray(r);

	}
	public static String[] removeElement(String[] arr, int index) {
		if (arr == null || index < 0 || index >= arr.length) {
			return arr;
		}
		String[] anotherArray = new String[arr.length - 1];
		for (int i = 0, k = 0; i < arr.length; i++) {
			if (i == index) {
				continue;
			}
			anotherArray[k++] = arr[i];
		}
		return anotherArray;
	}
	public static Vector<String> arrayVector(String[] arrStr) {
		Vector<String> r = new Vector<String>();
		for (String element : arrStr) {
			r.add(element);
		}
		return r;
	}
	public static String[] vectorArray(Vector<String> vecStr) {
		String[] r = new String[vecStr.size()];
		for (int i = 0; i < vecStr.size(); i++) {
			r[i] = vecStr.get(i);
		}
		return r;
	}
	//
}