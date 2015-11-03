package com.expedia.mongoMigration;

import com.mongodb.DBObject;

public class ConversionRunner {

	public static void main(String[] args) {
		MongoConnection source = null;
		MongoConnection destination = null;
		/*
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
		*/

		String migrationType = args[0];
		String sourceHostname = args[1];
		String destinationHostname = args[2];

		source = new MongoConnection(sourceHostname, "seyren", null, null);
		destination = new MongoConnection(destinationHostname, "seyren", null, null);

		if (migrationType == "migrateAndDisable" || migrationType == "migrate") {
			boolean disable = true;
			if (migrationType == "migrate") {
				disable = false;
			}
			boolean dropCurrentChecksSuccess = destination.dropCurrentChecks();
			DBObject[] sourceChecks = source.getChecks();
			for (DBObject check : sourceChecks){
				if (disable) {
					boolean disableSuccess = destination.disableCheck(check);
				}
				boolean convertSuccess = destination.convertCheckSubscriptions(check, disable);
				boolean saveSuccess = destination.saveCheck(check);
			}
		}
	}

}
