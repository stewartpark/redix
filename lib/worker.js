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

/*
    Worker
*/
var redis = require('redis');
var path = require('path');
var protocol = require(path.join(__dirname, 'protocol'));
var scale = require(path.join(__dirname, 'scale'));
var worker;

// Worker wrapping function.
function work(msg){
    worker(msg);
}

// Setup new connections.
function setupConn(cid, socket){
    var conn = global['conn'];
    var redisConnections = {};

    // Create connections
    for(var x in global['configurations']){
        x = global['configurations'][x];
        y = redis.createClient(x.port, x.host);
        y.debug_mode = true;

        // Install Event handlers
        y.on('subscribe', function(channel, count){
            work({instr: 'subscribe', channel: channel, count: count, connectionId: cid});
        });
        y.on('message', function(channel, message){
            work({instr: 'message', channel: channel, message: message, connectionId: cid});
        });
        redisConnections[x.host + ':' + x.port] = y;
    }

    conn[cid] = {
        redis: redisConnections,
        socket: socket,
        isMulti: false,
        multiQueue: [],
        channels_subscribed: 0
    }
}


// Free all of objects related this connection.
function cleanConn(cid){
    // Clean up all of redis connections
    var _redis = conn[cid]['redis'];
    for(var x in _redis){
        _redis[x].end();
    }

    delete conn[cid];
}

var worker = function(msg){
    var conn = global['conn'];
    var connId = msg.connectionId;

	function incr_defer_ok(){
		global['defer_ok']++;// parseInt(global['defer_ok']) + 1;
		console.log('incr_defer_ok(): DEFER_OK:', global['defer_ok']);
	}


    switch(msg.instr){
        case 'command': // Redis command
            var cmd = msg.cmd.toString().toUpperCase();
            var prm = msg.prm;
            var attr = protocol.getAttribute(cmd);
            var socket = conn[connId].socket;
			
			console.log('LOG:',msg.defer_id,cmd,prm, 'DEFER_OK:', global['defer_ok'], 'DEFER_COUNT:', global['defer_count']);

			if(!msg.isGoingToSplit) {
					if(global['defer_ok'] < msg.defer_id && !(conn[connId]['isMulti'] && !msg.dequeuing)) {
						console.log('isGoingToSplit', msg.isGoingToSplit, 'DEFERRED:', msg.defer_id, 'CURRENT defer_ok:', global['defer_ok']);
						process.nextTick(function() {
							worker(msg);
						});
						return;
					}
			}
			

            if(attr['handler']){
                // if it has own handler,
                attr['handler'](connId, prm);
				console.log('Handler called.')
				//incr_defer_ok();
            }else{
                // If it doesn't have,
                if(conn[connId]['isMulti'] && !msg.dequeuing){
                    // If atomicity is required, enqueue commands. (Just defer)
                    conn[connId].multiQueue.push({
                        cmd: cmd,
                        prm: prm,
						isGoingToSplit: msg.isGoingToSplit,
						defer_id: msg.defer_id
                    });
					console.log('QUEUED');
                    socket.write('+QUEUED\r\n');
					//incr_defer_ok();
                } else {
                   

                    // If not required,
                    if(attr['sharding']){
						var defer_id = msg.defer_id;
						var noOf = scale.redisNumberOfSplitable(connId, cmd, prm, attr);
                        var max = noOf;
                        var endOfList = false;
                        var i = 0;
                        chkSplit = scale.redisSplitIfPossible(connId, cmd, prm, attr, function(/*new_defer_id, */cmd, prm, attr){
                            if(++i == max){
                               endOfList = true; 
                            }
                            // Splitting commands
							console.log('SPLITTED:', cmd, prm, defer_id, 'noOf', noOf);
                            work({instr: 'command', cmd: cmd, prm: prm, endOfList: endOfList, startOfList: noOf, connectionId: connId, defer_id: defer_id++, dequeuing: msg.dequeuing});
							noOf = 0;
                        }/*,
                        function(){
                            // Launched when all callback has been generated.

							//if(!conn[connId].isMulti){
							//	console.log('Splited and Not on multi');
							incr_defer_ok();
							//}
                        }*/);
                        
                        // If it is not splittable, run
                        if(!chkSplit){
                            // If splitted, 
                            if(msg.startOfList){
                                console.log('!!!!!!!!!!!!!!!!!!!! start Of List:', msg.startOfList);
                                socket.write('*' + msg.startOfList + '\r\n');
                            }
                            
                            if(msg.endOfList){
                                console.log('!!!!!!!!!!!!!!!!!!!! end of List');
                                //incr_defer_ok();
                            }

                            var posHashKey = attr['sharding']['posHashKey'];
                            var host = ring.getNode(prm[posHashKey]);
                            // Send command
                            conn[connId]['redis'][host].send_command(cmd, prm, function(err, res){
                                if(err){
                                    // If error occurred.
                                    socket.write('-' + err + '\r\n');
									incr_defer_ok();
                                } else {
                                    protocol.encode(res, attr, function(result){
                                        socket.write(result);
										incr_defer_ok();
                                    });
                                }
                            });
                        }
                    } else {
                        // TODO If it's not shardable command, broadcast.
                        // cf) KEYS
						console.log('not shardable');
						incr_defer_ok();
                    }
                }
            }
            break;
        case 'message': // Channel message
            var channel = msg.channel.toString();
            var message = msg.message.toString();
            console.log("MESSAGE:", getByteLength(message), message);
            conn[connId].socket.write(
                "*3\r\n$7\r\nmessage\r\n$" + getByteLength(channel) + "\r\n" + channel + "\r\n$" + getByteLength(message) + "\r\n" + message + "\r\n"
            );

            break;
        case 'subscribe': // Subscribed channel (count)
            var channel = msg.channel.toString();
            var count = msg.count;

            conn[connId].socket.write(
                '*3\r\n$9\r\nsubscribe\r\n$' + getByteLength(channel) + '\r\n' + channel + '\r\n:' + count + '\r\n'
            );
            break;
    }
};

function getByteLength( data ){
   return Buffer(data).length; //(data.length+(escape(data)+"%u").match(/%u/g).length-1);
}

exports.work = work;
exports.setupConn = setupConn;
exports.cleanConn = cleanConn;
