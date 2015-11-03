package com.expedia.mongoMigration;

import com.mongodb.DBObject;

public class ConversionRunner {

	public static void main(String[] args) {
		MongoConnection worker = null;
		String databaseHost = args[0];
		String databaseName = args[1];
		if (args.length > 2){
			String userName = args[2];
			String password = args[3];
			worker = new MongoConnection(databaseHost, databaseName, userName, password);
		}
		else {
			worker = new MongoConnection(databaseHost, databaseName, null, null);
		}
		boolean dropCurrentChecksSuccess = worker.dropCurrentChecks();
		DBObject[] oldChecks = worker.getOldChecks();
		for (DBObject check : oldChecks){
			//boolean deleteSuccess = worker.deleteOldCheck(check);
			//boolean updateIDSuccess = worker.updateCheckID(check);
			boolean disableSuccess = worker.disableCheck(check);
			boolean convertSuccess = worker.convertCheckSubscriptions(check);
			boolean saveSuccess = worker.saveCheck(check);
		}
	}

}
