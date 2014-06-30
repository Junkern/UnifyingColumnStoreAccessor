package interfaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Row;


public interface MiddlewareInterface {
	/**
	 * Create a table with given name and indexed column.
	 * @param tableName
	 * @param primaryKey DynamoDb: indexed primary key; Hypertable: indexed column family
	 */
	public void createTable(String tableName, String primaryKey);
	public void deleteTable(String tableName);
	/**
	 * Insert rows into the table with given name.
	 * @param tableName
	 * @param rows
	 */
	public void insertRows(String tableName, List<Row> rows);
	public Row getRowByKey(String tableName, Map<String, String> combinedKey);
	public List<Row> getRowsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys);
	public List<Row> getRows(String tableName, String conditionalOperator, List<Filter> filters);
	public List<String> getTableNames();
}
