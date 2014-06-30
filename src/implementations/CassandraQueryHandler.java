package implementations;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import model.Attribute;
import model.Filter;
import model.Row;

public class CassandraQueryHandler {
	
	/**
	 * Creates a table in a keyspace. Everything is saved as a blob, 
	 * so the client has to handle the converting from and to bytes.
	 * 
	 * @param keyspace The name of the keyspace
	 * @param tableName The name of the table to be created
	 * @param columns The columns of the table to be created. 
	 * @param primaryKeys The primary key(s) of the table. Those are the name of the columns which make up the primary key
	 */
	public static void createTable(String keyspace, String tableName, List<String> columns, List<String> primaryKeys) {
		String query = "CREATE TABLE IF NOT EXISTS ";
		query += keyspace+"."+tableName+" (";
		for(String column : columns) {
			query += column +" blob,";
		}
		query += "PRIMARY KEY (";
		for (String column : primaryKeys) {
			query += column +",";
		}
		query.substring(0, query.length()-1);
		query += ");";
		CassandraHandler.session.execute(query);
	}
	
	/**
	 * Deletes a table in the specified keyspace
	 * 
	 * @param keyspace The keyspace of the table
	 * @param table The table to be deleted
	 */
	public static void deleteTable(String keyspace, String table) {
		String query = "DROP TABLE IF EXISTS "+keyspace+"."+table;
		session.execute(query);
	}
	
	/**
	 * Fetches the table names of the current keyspace
	 * @return A list of all table names.
	 */
	public static List<String> getTableNames() {
		String query = "DESCRIBE TABLES";
		ArrayList<String> tableNames = new ArrayList<String>(); 
		ResultSet results = CassandraHandler.session.execute(query);
		for(Row row : results) {
			tableNames.add(row.getString(0));
		}
		return tableNames;
	}
	
	/**
	 * Inserts data into the table of the keyspace.
	 * 
	 * @param keyspace The keyspace of the table
	 * @param table The table where the data is inserted
	 * @param columns The columns the row has. The primary key columns have to be provided
	 * @param values The values of the columns
	 */
	public static void insertItems(String keyspace, String table, List<Row> items) {
		for(Row item : items) {
			insertItem(keyspace, table, item);
		}
	}
	
	public static void insertItem(String keyspace, String table, Row item) {
		String query = "INSERT INTO "+keyspace+"."+table+" (";
		Collection<Attribute> attributes = item.getAttributes();
		for (Attribute attribute : attributes) {
			query += " "+attribute.getName()+", ";
		}
		query = query.substring(0, query.length()-1);
		query += " VALUES (";
		for (Attribute attribute : attributes) {
			query += " "+attribute.getValue()+", ";
		}
		query = query.substring(0, query.length()-1);
		CassandraHandler.session.execute(query);
	}
	
	public static Row getRowByKey(String keyspace, String tableName, Map<String, String> combinedKey) {
		//TODO what about selecting only a few columns and not all?
		String query = "SELECT * ";
//		if (columnsToSelect == null || columnsToSelect.isEmpty()) {
//			query += " * ";
//		} else {
//			for (String column : columnsToSelect) {
//				query += " "+column+" ,";
//			}
//			query = query.substring(0,query.length()-1);
//		}
		query += " FROM "+keyspace+"."+table;
		query += " WHERE ";
		for(String key : combinedKey.keySet()) {
			query += key+" = "+combinedKey.get(key)+" AND ";
		}
		query = query.substring(0, query.length()-4);
		
		ResultSet results = CassandraHandler.session.execute(query);
		Row row = results.one();
		//TODO implement getting the values
		
		List<String> columnNames = getColumnNamesOfResultSet(results);
		for(Row row : results.all()) {
			for (String column : columnNames) {
				ByteBuffer value = row.getBytes(column);
			}
		}
	}
	
	public static List<Row> scanTable(String keyspace, String tableName, String conditionalOperator, List<Filter> filters) {
		String query = "SELECT * FROM" + keyspace + "." + tableName + " WHERE ";
		for (Filter filter : filters) {
			query += filter.getAttributeName()+" "+filter.getComparisonOperator()+" "+filter.getAttributeValue();
			query += " "+conditionalOperator+" ";
		}
		query = query.substring(0, query.length()-conditionalOperator.length()-1);
		ResultSet results = CassandraHandler.session.execute(query);
		//TODO implement getting the values
	}
}
