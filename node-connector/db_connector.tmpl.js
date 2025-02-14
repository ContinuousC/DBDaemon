/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

const {CborRpcClient} = require('rpc');

class DBConnector extends CborRpcClient {

    async connect(socketPath) {

	await super.connect();
	
	if (this.verbose) {
	    console.log("Waiting for database...");
	}

	try {

	    await this.wait_for_databases();

	    if (this.verbose) {
		console.log("Done!");
	    }

	} catch (e) {
	    if (this.verbose) {
		console.log('Error: ' + e);
	    }
	    throw e;
	}
    }

    {{DbService}}

}

module.exports = {DBConnector};
