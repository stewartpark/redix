/*
   REDIX, the redis multiplexing, sharding proxy.

   Designed by Juwhan Kim, Ju-yeong Park
   Coded by Ju-yeong park

   Copyright (c) 2011, Sho-U Communications
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
   1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
   2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
   3. All advertising materials mentioning features or use of this software
   must display the following acknowledgement:
   This product includes software developed by the Sho-U Communications.
   4. Neither the name of the Sho-U Communications nor the
   names of its contributors may be used to endorse or promote products
   derived from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY SHO-U COMMUNICATIONS ''AS IS'' AND ANY
   EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL SHO-U COMMUNICATIONS BE LIABLE FOR ANY
   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */

// Globally coupled variables. (I THINK THIS IS A BAD DESIGN DECISION, EITHER.)
// {
// Variables used globally
    global['configurations'] = [];
    global['conn'] = {};

    // Count variables
    global['ident_count'] = 0;
    global['defer_count'] = 0;
    global['defer_ok'] = 0;
// }


// Variables
var server_port = 6379; // Default

// Libraries
var fs = require('fs'),
    util = require('util'),
    net = require('net'),
    path = require('path'),
    hash_ring = require('hash_ring'),
    protocol = require(path.join(__dirname, 'lib', 'protocol')),
    scale = require(path.join(__dirname, 'lib', 'scale')),
    worker = require(path.join(__dirname, 'lib', 'worker')),
    argv = require('optimist').argv;

/*
   Boot procedure.
 */
function boot() {
    var hosts = {};
    /*
       Load settings
     */
    if(argv.p)
        server_port = parseInt(argv.p);

    var setting_file = argv._[0];
    if(!setting_file){
        // If the parameter is not specified,
        setting_file = "/etc/redix.cfg"; 
    }
    global['configurations'] = [];

    fs.readFile(setting_file, function(err, data){
            var tmp;

            if(err){
                util.debug('[ERR] Configuration file not found.');
                process.exit(1);
            }
            hosts = {};
            // If it is successfully loaded,
            // Parse settings.
            tmp = data.toString().replace('\r', '').split('\n');
            for(var v in tmp){
                v = tmp[v].split(' ');
                if(!v[0]) continue;
                if(!v[1]){
                    // If the port is not specified,
                    v[1] = 6379;
                }
                hosts[v[0] + ':' + v[1]] = 1;
                // Save configurations
                global['configurations'].push( {host: v[0], port: v[1], auth: v[2]} );
                console.log('Host ' + v[0] + ':' + v[1] + ' loaded.');
            }

            global['ring'] = new hash_ring(hosts);

            // Initialization done.
            net.createServer(function(c) { 
                    // Generate new identity for new connection.
                    var my_ident = global['ident_count']++;
                    // Initialize the connection.
                    //work({instr: 'setup', connectionId: my_ident, socket: c});
                    worker.setupConn(my_ident, c);

                    c.on('close', function(){
                        // Clean the connection.
                        //work({instr: 'clean', connectionId: my_ident});
                        worker.cleanConn(my_ident);
                        });
                    c.on('error', function(exception){
                            console.log(exception);
                        });

                    c.on('data', function(data){
                        // Tokenize input stream.
                        protocol.decode(data.toString(), function(x) {
                            var cmd, prm;
                            cmd = x[0];
                            prm = x.splice(1);

                            // Split commands for scale-out.
                            // Send this job to worker.
							var nuCount = scale.redisNumberOfSplitable(my_ident, cmd, prm, protocol.getAttribute(cmd));
							var isGoingToSplit = (nuCount > 1);
							console.log('EMIT COMMAND:', cmd , 'prm', prm, 'defer_count:', global['defer_count'],'+', nuCount, '  isGoingToSplit ',isGoingToSplit); 
                            worker.work({instr: 'command', cmd: cmd, prm: prm, connectionId: my_ident, defer_id: defer_count, isGoingToSplit: isGoingToSplit});
							//if(global['conn'][my_ident].isMulti){
							//	global['defer_count'] ++;
							//} else {
                            if(!protocol.getAttribute(cmd)['handler']){
								global['defer_count'] += nuCount == 1 ? 1 : nuCount;
                            }
							//}
                            });
                        });
            }).listen(server_port);
    });
}


boot();
