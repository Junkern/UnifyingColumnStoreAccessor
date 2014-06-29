package implementations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Item;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;


public class DynamoDbQueryHandler {

	// Provide initial provisioned throughput values as Java long data types
	private static ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT =
			new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L);

	/**
	 * Create a table with the given hashKey as row id
	 * 
	 * @param tableName
	 * @param primaryKey
	 */
	public static void createTable(String tableName, String primaryKey) {
		ArrayList<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();

		ks.add(new KeySchemaElement().withAttributeName(
				primaryKey).withKeyType(KeyType.HASH));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				primaryKey).withAttributeType("S"));

		CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName)
				.withKeySchema(ks)
				.withProvisionedThroughput(DEFAULT_PROVISIONED_THROUGHPUT);

		request.setAttributeDefinitions(attributeDefinitions);
		try {
			DynamoDbHandler.CLIENT.createTable(request);
		}
		catch (ResourceInUseException e) {
			System.err.println("Table '" + tableName + "' already exists");
		}
	}

	public static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
			String hashKeyName, String hashKeyType, String rangeKeyName, String rangeKeyType) {
	}
	
	public static ListTablesResult listTables(){
		return DynamoDbHandler.CLIENT.listTables();
	}

	/**
	 * Delete the table with given name
	 * 
	 * @param tableName
	 */
	public static void deleteTable(String tableName) {
		DynamoDbHandler.CLIENT.deleteTable(new DeleteTableRequest(tableName));
	}

	/**
	 * Insert values into the given table. Items are stored as blobs in the
	 * database.
	 * 
	 * @param tableName
	 * @param items
	 */
	public static void insertItems(String tableName, List<Item> items) {
		Map<String, AttributeValue> transformedItem = new HashMap<>();
		for(Item item : items){
			for(String name : item.getAttributes().keySet()){
				transformedItem.put(name, new AttributeValue().withS(item.getAttributes().get(name)));
			}
			PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(transformedItem);
			DynamoDbHandler.CLIENT.putItem(itemRequest);
			transformedItem.clear();
		}
		
		
//		
//		for (Map<String, String> item : items) {
//			for (String key : item.keySet()) {
//				transformedItem.put(key, new AttributeValue().withB(ByteBuffer.wrap(item.get(key).getBytes())));
//			}
//			PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(transformedItem);
//			DynamoDbHandler.CLIENT.putItem(itemRequest);
//			transformedItem.clear();
//		}
	}

	/**
	 * Get item of table with provided key-value.
	 * 
	 * @param tableName
	 * @param combinedKey
	 * @return
	 */
	public static Item getItemByKey(String tableName, Map<String, String> combinedKey) {
		Map<String, AttributeValue> transformedKey = new HashMap<>();
		for (String key : combinedKey.keySet()) {
			transformedKey.put(key, new AttributeValue().withS(combinedKey.get(key)));
		}

		GetItemResult result = DynamoDbHandler.CLIENT.getItem(new GetItemRequest(tableName, transformedKey));

		Map<String, String> attributes = new HashMap<>();
		for (String resultKey : result.getItem().keySet()) {
			attributes.put(resultKey, result.getItem().get(resultKey).getS());
		}

		return new Item(attributes);
	}

	/**
	 * Get items from different tables by values of their key.
	 * 
	 * @param tableName
	 * @param combinedKey
	 * @return
	 */
	public static List<Item> getItemsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		HashMap<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();

		for (String tableName : tableNamesWithKeys.keySet()) {
			ArrayList<Map<String, String>> keyList = tableNamesWithKeys.get(tableName);

			ArrayList<Map<String, AttributeValue>> transformedKeyList = new ArrayList<>();
			for (Map<String, String> keyValueMap : keyList) {
				Map<String, AttributeValue> transformedItem = new HashMap<>();
				for (String key : keyValueMap.keySet()) {
					transformedItem.put(key, new AttributeValue().withS(keyValueMap.get(key)));
				}
				transformedKeyList.add(transformedItem);
			}
			requestItems.put(tableName, new KeysAndAttributes().withKeys(transformedKeyList));
		}

		BatchGetItemResult result = DynamoDbHandler.CLIENT.batchGetItem(new BatchGetItemRequest().withRequestItems(requestItems));

		List<Item> items = new ArrayList<>();
		for (String tableName : tableNamesWithKeys.keySet()) {
			List<Map<String, AttributeValue>> tableResults = result.getResponses().get(tableName);
			Map<String, String> attributes = null;

			for (Map<String, AttributeValue> tableResult : tableResults) {
				attributes = new HashMap<>();
				for (String key : tableResult.keySet()) {
					attributes.put(key, tableResult.get(key).getS());
				}
				items.add(new Item(attributes));
			}
		}
		return items;
	}

	/**
	 * Returns the the result of the scanned table with the applied filters. The
	 * filters are connected via the conditionalOperator. It can either be AND
	 * or OR.
	 * 
	 * @param tableName The table to be scanned.
	 * @param filters A list with Filter objects.
	 * @param conditionalOperator
	 *            AND | OR - Conditional clauses in DynamoDb can either have AND
	 *            or OR connectors, no mix.
	 * @return
	 */
	public static List<Item> scanTable(String tableName, List<Filter> filters, String conditionalOperator) {
		Map<String, Condition> scanFilter = new HashMap<>();
		for (Filter filter : filters) {
			scanFilter.put(filter.getAttributeName(),
					new Condition()
							.withComparisonOperator(filter.getComparisonOperator())
							.withAttributeValueList(new AttributeValue().withS(filter.getAttributeValue())));
		}
		ScanResult scanResult = DynamoDbHandler.CLIENT.scan(new ScanRequest(tableName)
				.withConditionalOperator(conditionalOperator).withScanFilter(scanFilter));

		ArrayList<Item> items = new ArrayList<>();
		Map<String, String> attributes = null;
		for (Map<String, AttributeValue> result : scanResult.getItems()) {
			attributes = new HashMap<>();
			for (String key : result.keySet()) {
				attributes.put(key, result.get(key).getS());
			}
			items.add(new Item(attributes));
		}

		return items;
	}
}
