import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import middleware.QueryHandler;
import middleware.UnifyingMiddleware;
import model.Item;


public class TestHandler {

	public static String TABLE_NAME = "test_mushroom_kingdom";
	public static String TABLE_NAME2 = "test_hyrule";
	public static QueryHandler queryHandler = UnifyingMiddleware.getQueryHandler();

	public static void insertTestItems(String tableName) {
		List<Item> items = new ArrayList<>();
		Map<String, String> attributes = new HashMap<>();

		if (tableName.equals(TABLE_NAME)) {
			attributes.put("id", "1");
			attributes.put("name", "Mario");
			items.add(new Item(attributes));

			attributes = new HashMap<String, String>();
			attributes.put("id", "2");
			attributes.put("name", "Bowser");
			attributes.put("type", "turtle");
			items.add(new Item(attributes));

			attributes = new HashMap<String, String>();
			attributes.put("id", "3");
			attributes.put("name", "Peach");
			attributes.put("type", "princess");
			attributes.put("age", "23");
			items.add(new Item(attributes));

			attributes = new HashMap<String, String>();
			attributes.put("id", "4");
			attributes.put("name", "Daisy");
			attributes.put("type", "princess");
			attributes.put("age", "25");
			items.add(new Item(attributes));

			attributes = new HashMap<String, String>();
			attributes.put("id", "5");
			attributes.put("name", "Yoshi");
			attributes.put("type", "dinosaur");
			attributes.put("age", "42");
			items.add(new Item(attributes));
		}
		else if (tableName.equals(TABLE_NAME2)) {
			attributes.put("id", "100");
			attributes.put("name", "Link");
			attributes.put("status", "hero of time");
			items.add(new Item(attributes));

			attributes = new HashMap<String, String>();
			attributes.put("id", "200");
			attributes.put("name", "Ganondorf");
			attributes.put("alias", "Ganon");
			items.add(new Item(attributes));
		}
		queryHandler.insertItems(tableName, items);
	}

	public static void deleteTestTables() {
		queryHandler.deleteTable(TABLE_NAME);
		queryHandler.deleteTable(TABLE_NAME2);
	}

	public static void createTestTables() {
		queryHandler.createTable(TABLE_NAME, "id");
		queryHandler.createTable(TABLE_NAME2, "id");
	}

	public static List<String> getTestTableNames(List<String> tableNames) {
		List<String> testTableNames = new ArrayList<>();
		for (String tableName : tableNames) {
			if (tableName.indexOf("test_") > -1 && tableName.indexOf("^test_") == -1) {
				testTableNames.add(tableName);
			}
		}

		return testTableNames;
	}
	
	public static Map<String, ArrayList<Map<String, String>>> createKeys(){
		Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys = new HashMap<>();
		ArrayList<Map<String, String>> keyList = new ArrayList<>();

		Map<String, String> keys = new HashMap<>();
		keys.put("id", "1");
		keyList.add(keys);

		keys = new HashMap<>();
		keys.put("id", "2");
		keyList.add(keys);
		tableNamesWithKeys.put(TestHandler.TABLE_NAME, keyList);

		keys = new HashMap<>();
		keys.put("id", "100");
		keyList.add(keys);

		keys = new HashMap<>();
		keys.put("id", "200");
		keyList.add(keys);
		tableNamesWithKeys.put(TestHandler.TABLE_NAME2, keyList);
		return tableNamesWithKeys;
	}
}
