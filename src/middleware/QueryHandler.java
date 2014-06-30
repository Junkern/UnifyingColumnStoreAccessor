package middleware;
import implementations.DynamoDbQueryHandler;
import implementations.HypertableQueryHandler;
import interfaces.MiddlewareInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Row;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

/**
 * {@inheritDoc}
 * @author Benjamin Räthlein
 *
 */
public class QueryHandler implements MiddlewareInterface {

	@Override
	public void createTable(String tableName, String primaryKey) {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			DynamoDbQueryHandler.createTable(tableName, primaryKey);
			break;
		case Hbase:
			break;
		case Hypertable:
			HypertableQueryHandler.createTable(tableName, primaryKey);
			break;
		}
	}

	@Override
	public List<String> getTableNames() {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			ListTablesResult result = DynamoDbQueryHandler.listTables();
			return result.getTableNames();
		case Hbase:
			break;
		case Hypertable:			
			return HypertableQueryHandler.listTables();
		}
		return null;
	}

	@Override
	public void deleteTable(String tableName) {
		try {
			switch (Configurator.getUsedDatabase()) {
			case Cassandra:
				break;
			case DynamoDb:
				DynamoDbQueryHandler.deleteTable(tableName);
				break;
			case Hbase:
				break;
			case Hypertable:
				HypertableQueryHandler.deleteTable(tableName);
				break;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertRows(String tableName, List<Row> items) {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			DynamoDbQueryHandler.insertItems(tableName, items);
			break;
		case Hbase:
			break;
		case Hypertable:
			HypertableQueryHandler.insertItems(tableName, items);
			break;
		}
	}

	@Override
	public Row getRowByKey(String tableName, Map<String, String> combinedKey) {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			return DynamoDbQueryHandler.getRowByKey(tableName, combinedKey);
		case Hbase:
			break;
		case Hypertable:
			String key = combinedKey.entrySet().iterator().next().getValue();
			return HypertableQueryHandler.getRowByKey(tableName, key);		
		}

		return null;
	}	

	//TODO: allow select only from one table and not set of tables
	@Override
	public List<Row> getRowsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			return DynamoDbQueryHandler.getItemsByKeys(tableNamesWithKeys);
		case Hbase:
			//return HypertableQueryHandler.getRowsByKeys(tableNamesWithKeys.get(0));
		case Hypertable:
			break;
		}
		return null;
	}	
	

	@Override
	public List<Row> getRows(String tableName, String conditionalOperator, List<Filter> filters) {
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			return DynamoDbQueryHandler.scanTable(tableName, filters, conditionalOperator);
		case Hbase:
			break;
		case Hypertable:
			return HypertableQueryHandler.scanTable(tableName, conditionalOperator, filters);
		}

		return null;
	}

}
