import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Item;

import org.junit.Test;


public class DynamoDbTest {

	@Test
	public void createTableTest() {
		TestHandler.deleteTestTables();
		assertEquals(0, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
		TestHandler.createTestTables();
		assertEquals(2, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
	}

	@Test
	public void getItemTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Map<String, String> key = new HashMap<>();
		key.put("id", "2");

		Item item = TestHandler.queryHandler.getItemByKey(TestHandler.TABLE_NAME, key);
		assertEquals("Bowser", item.getAttributes().get("name"));
		assertEquals(true, item.getAttributes().containsKey("type"));
	}

	@Test
	public void getBatchItemTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		TestHandler.insertTestItems(TestHandler.TABLE_NAME2);
		List<Item> items = TestHandler.queryHandler.getItemsByKeys(TestHandler.createKeys());
		
		assertEquals(4, items.size());		
		boolean mario = false;
		boolean link = false;
		boolean bowser = false;
		boolean ganondorf = false;
		
		for(Item item : items){
			if(item.getAttributes().get("name").equals("Mario")){
				assertEquals(false, item.getAttributes().containsKey("type"));
				mario = true;
			}
			else if(item.getAttributes().get("name").equals("Link")){
				assertEquals("hero of time", item.getAttributes().get("status"));
				link = true;
			}
			else if(item.getAttributes().get("name").equals("Bowser")){
				assertEquals(true, item.getAttributes().containsKey("type"));
				bowser = true;
			}
			else if(item.getAttributes().get("name").equals("Ganondorf")){
				assertEquals(true, item.getAttributes().containsKey("alias"));
				assertEquals("200", item.getAttributes().get("id"));		
				ganondorf = true;
			}
		}
		
		assertEquals(true, mario && link && bowser && ganondorf);
	}
	
	@Test
	public void scanTest(){
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		
		ArrayList<Filter> filters = new ArrayList<>();
		Filter filter = new Filter("age", "GE", "24");
		filters.add(filter);
		filter = new Filter("type", "EQ", "princess");
		filters.add(filter);
		
		List<Item> items = TestHandler.queryHandler.getItems(TestHandler.TABLE_NAME, "OR", filters);
		
		assertEquals(3, items.size());
		assertEquals("Daisy", items.get(0).getAttributes().get("name"));
		assertEquals("Peach", items.get(1).getAttributes().get("name"));
		assertEquals(true, items.get(1).getAttributes().containsKey("type"));
		assertEquals("42", items.get(2).getAttributes().get("age"));
		assertEquals("Yoshi", items.get(2).getAttributes().get("name"));
		assertEquals(false, items.get(1).getAttributes().containsKey("alias"));
		
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		
		filters = new ArrayList<>();
		filter = new Filter("age", "GE", "24");
		filters.add(filter);
		filter = new Filter("type", "EQ", "princess");
		filters.add(filter);
		
		items = TestHandler.queryHandler.getItems(TestHandler.TABLE_NAME, "AND", filters);
		
		assertEquals(1, items.size());
		assertEquals("Daisy", items.get(0).getAttributes().get("name"));
	}
}
