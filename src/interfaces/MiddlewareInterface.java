package interfaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Item;


public interface MiddlewareInterface {
	public void createTable(String tableName, String primaryKey);
	public void deleteTable(String tableName);
	public void insertItems(String tableName, List<Item> items);
	public Item getItemByKey(String tableName, Map<String, String> combinedKey);
	public List<Item> getItemsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys);
	public List<Item> getItems(String tableName, String conditionalOperator, List<Filter> filters);
	public List<String> getTableNames();
}
