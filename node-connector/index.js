var { DBConnector } = require("./db_connector");
const monitored_hosts = require("./monitored-hosts.json");
const monitored_host_value = require("./monitored-hosts-value.json");

function sleep(ms) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

async function run(name, fun) {
    console.log("Running " + name);
    const res = await fun();
    console.log("Result: " + JSON.stringify(res));
    return res;
}

async function main() {
    db = new DBConnector({
	ca_path: "/home/mdp/projects/Rpc/Source/Rust/rpc/tests/certs/ca.crt",
	cert_path: "/home/mdp/projects/Rpc/Source/Rust/rpc/tests/certs/client.crt",
	key_path: "/home/mdp/projects/Rpc/Source/Rust/rpc/tests/certs/client.key",
	verbose: true
    });
    await db.connect();
    console.log("connected to database daemon");

    try {

	try {    
	    await run("unregister_table", () =>
		db.unregister_table("monitored-hosts-test"));
	} catch (e) {}
	await run("register_table", () =>
	    db.register_table("monitored-hosts-test", monitored_hosts));

	let object_id = await run("create_config_object", () =>
	    db.create_config_object(
		"monitored-hosts-test",
		monitored_host_value,
		true));

	await sleep(1);

	await run("update_config_object", () =>
	    db.update_config_object(
		"monitored-hosts-test",
		object_id,
		monitored_host_value,
		false));

	await sleep(1);

	await run("activate_config_object", () =>
	    db.activate_config_object(
		"monitored-hosts-test",
		object_id));

	await sleep(1);

	await run("update_config_object", () =>
	    db.update_config_object(
		"monitored-hosts-test",
		object_id,
		monitored_host_value,
		false));

	await run("read_config_object", () =>
	    db.read_config_object(
		"monitored-hosts-test",
		object_id,
		"current"));

	await run("remove_config_object", () =>
	    db.remove_config_object(
		"monitored-hosts-test",
		object_id));

	try {
	    await run("read_config_object", () =>
		db.read_config_object(
		    "monitored-hosts-test",
		    object_id,
		    "current"));
	} catch (e) {
	    console.log('Failed to read current value (as expected): '
			+ e.toString());
	}

	await run("read_config_object", () =>
	    db.read_config_object(
		"monitored-hosts-test",
		object_id,
		"active"));
	
	await sleep(1);

	await run("activate_config_object", () =>
	    db.activate_config_object(
		"monitored-hosts-test",
		object_id));

	try {
	    await run("read_config_object", () =>
		db.read_config_object(
		    "monitored-hosts-test",
		    object_id,
		    "active"));
	} catch (e) {
	    console.log('Failed to read current value (as expected): '
			+ e.toString());
	}
	
	await run("read_config_object_history", () =>
	    db.read_config_object_history(
		"monitored-hosts-test",
		object_id,
		"current",
		{}));

	await run("read_config_object_history", () =>
	    db.read_config_object_history(
		"monitored-hosts-test",
		object_id,
		"active",
		{}));

	const filter = { at: {
	    path: [{field: "ip_monitoring_management"}],
	    filter: {eq: "127.0.0.1"}
	}};
	
	await run("query_config_objects_history", () =>
	    db.query_config_objects_history(
		"monitored-hosts-test",
		filter,
		"current",
		{}));

	await run("query_config_objects_history", () =>
	    db.query_config_objects_history(
		"monitored-hosts-test",
		filter,
		"active",
		{}));

    } catch (e) {
	console.log('Error: ' + e);
    }

    process.exit(0);

    // now = new Date().toISOString()
	// console.log("now: " + now)
	// xDaysAgo = new Date()
	// xDaysAgo.setDate(xDaysAgo.getDate() - 2)
	// xDaysAgo = xDaysAgo.toISOString()

	// ipObj = require("../Rust/examples/testobj.json");
	// ipObj["now"] = now;
	// ipObjChanged = require("../Rust/examples/testobj_changed.json");
	// ipObjChanged["now"] = xDaysAgo;
	// metricSchema = require("../Rust/examples/metricSchema.json");
	// metricObj = require("../Rust/examples/metricObj.json");
	// metricObj["now"] = now;

	try {
		/*
		await db.register_metric_schema("load15", metricSchema);
		await db.create_metric("load15", metricObj);
		await sleep(1000);
		console.log(await db.read_metric("load15", {
			"match": {
				"subject": "host",
				"operator": "eq",
				"comparand": "kurumi.vst"
			}
		}, null, null))
		*/

		// creation
		let id = await db.create_object("ipMib.json", ipObj, true);
		console.log("id of the genrated object: " + id);
		id = await db.create_object("ipMib.json", ipObjChanged);
		console.log("id of the genrated object: " + id);
		/*
		console.log(await db.create_or_update_object(
			"ipMib.json", ipObj
		))
		*/
		await sleep(1000)
		/*
		console.log(await db.read_changes("ipMib.json", {
			"host_id": "kazuko.vst"
		}, xDaysAgo, now));
		*/

		// read
		// by id
		console.log(await db.read_object("ipMib.json", {
			"host_id": "kazuko.vst"
		}))
		/*
		// update
		console.log("updating object: ");
		ipObj["now"] = xDaysAgo;
		console.log(await db.update_object(
			"ipMib.json", ipObj, true
		))

		// by range
		console.log("using range: " + xDaysAgo + " -- " + now + ": " +
			getSize(await db.read_objects("ipMib.json", {
				"range": {
					"subject": "now",
					"from": {
						"date_time": xDaysAgo
					},
					"to": {
						"date_time": now
					},
				}
			}, null, null)))
		// last
		console.log("last: " +
			await db.read_objects("ipMib.json", "last", null, 3))



		// match
		// eq
		console.log("match eq: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "host_id",
					"operator": "eq",
					"comparand": "kazuko.vst"
				}
			}, null, null)))
		// Neq
		console.log("match neq: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "host_id",
					"operator": "neq",
					"comparand": "kazuko.vst"
				}
			}, null, null)))
		// like
		console.log("match like: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "host_id",
					"operator": "like",
					"comparand": "vst"
				}
			}, null, null)))
		// gt
		console.log("match gt: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "now",
					"operator": "gt",
					"comparand": now
				}
			}, null, null)))
		// gte
		console.log("match gte: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "now",
					"operator": "gte",
					"comparand": now
				}
			}, null, null)))
		// lt
		console.log("match lt: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "now",
					"operator": "lt",
					"comparand": now
				}
			}, null, null)))
		// lte
		console.log("match lte: " +
			getSize(await db.read_objects("ipMib.json", {
				"match": {
					"subject": "now",
					"operator": "lte",
					"comparand": now
				}
			}, null, null)))
		// all
		console.log("all: " +
			getSize(await db.read_objects("ipMib.json", {
				"all": {"filters": [
					{"match": {
						"subject": "host_id",
						"operator": "eq",
						"comparand": "kazuko.vst"
					}},
					{"match": {
						"subject": "host_id",
						"operator": "eq",
						"comparand": "vst"
					}}
				]}
			})))
		// any
		console.log("any: " +
			getSize(await db.read_objects("ipMib.json", {
				"any": {"filters": [
					{"match": {
						"subject": "host_id",
						"operator": "eq",
						"comparand": "kazuko.vst"
					}},
					{"match": {
						"subject": "host_id",
						"operator": "eq",
						"comparand": "vst"
					}}
				]}
			})))
	*/
	} catch (e) {
		console.log("caught an error");
		console.log(e);
		process.exit(1)
	}
	process.exit(0)
}

function getSize(xs) {
	x = 0
	xs.forEach((item, i) => {
		x += 1
	});
	return x
}

main();
