var http = require('http');
var net = require('net');

var client = net.createConnection("/var/test/db")

 http.createServer(function (req, res) {
	res.writeHead(200, {'Content-Type': 'text/plain'});
	res.end('Hello World!');
}).listen(8080);
