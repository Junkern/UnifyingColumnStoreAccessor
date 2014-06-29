package implementations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Item;

import org.apache.thrift.TException;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult;

public class HypertableQueryHandler {

	public static void createTable(String tableName, String indexedColumnFamily) {
		try {
			String queryString = String.format("CREATE TABLE %s (%s, INDEX %2$s)", tableName, indexedColumnFamily);
			System.out.println(queryString);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
	}

	public static List<String> listTables() {
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, "show tables");
			return result.getResults();
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static void deleteTable(String tableName){
		try {
			String queryString = String.format("DROP TABLE IF EXISTS %s", tableName);
			System.out.println(queryString);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
		}
		catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}

	public static void insertItems(String tableName, List<Item> items) {
		for (Item item : items) {
			String keyValue = item.getAttributes().get("id");
			for (String name : item.getAttributes().keySet()) {
				if (!name.equals("id")) {
					String queryString = String.format("INSERT INTO %s VALUES (\"%s\", \"id:%s\", \"%s\")", tableName, keyValue, name, item.getAttributes().get(name));
					try {
						HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
					}
					catch (ClientException | TException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static List<Item> scanTable(String tableName, String conditionalOperator, List<Filter> filters) {
		String  whereClause = "";
		for(Filter filter : filters){
			if(!whereClause.equals("")){
				whereClause += " " + conditionalOperator + " ";
			}
			whereClause = "" + filter.getAttributeName() + " " + filter.getComparisonOperator() + " \"" + filter.getAttributeValue() + "\"";
		}
		System.out.println(whereClause);
		String queryString = String.format("SELECT * FROM %s WHERE %s", tableName, whereClause);
		
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
			List<Item> transformedResultList = new ArrayList<>();
			Map<String, String> attributes = null;
			
			for(Cell cell : result.getCells()){
				attributes = new HashMap<>();
				attributes.put(cell.key.column_qualifier.toString(), new String(cell.getValue()));
				transformedResultList.add(new Item(attributes));
			}
			return transformedResultList;
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}
}
