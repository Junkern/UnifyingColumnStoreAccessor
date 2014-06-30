package interfaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Row;


public interface MiddlewareInterface {
	public void createTable(String tableName, String primaryKey);
	public void deleteTable(String tableName);
	public void insertItems(String tableName, List<Row> items);
	public Row getItemByKey(String tableName, Map<String, String> combinedKey);
	public List<Row> getItemsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys);
	public List<Row> getItems(String tableName, String conditionalOperator, List<Filter> filters);
	public List<String> getTableNames();
}
