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
    Redis Protocol
*/

var path = require('path'),
    worker = require(path.join(__dirname, 'worker'));

var redisResponseTypes = [
    'OK',
    'QUEUED'
];

var redisVariableTypes = [
    'string',
    'hash',
    'zset',
    'set',
    'list',
    'none',
];

var redisDefaultCommandAttribute = {
    // Some of commands should be treated as integer for parseInt'able values.
    canBeInteger: true,
    // For some of commands, the number of parameters are not limited, and works as multiple commands.
    split: false,
    // To scale-out, For redis, at present, sharding is needed. but for some of commands(keys, psubscribe, ...), it should be distributed to all servers.
    sharding : {
        posHashKey: 0
    }
};

var redisSpecialCommandAttribute = {
    'GET': {
        canBeInteger: false,
    },
    'MGET': {
        split: {
            start: 0,
            length: 1,
            to: 'GET'
        }
    },
    'MSET': {
        split: {
            start: 0,
            length: 2,
            to: 'SET'
        }
    },
    'MSETNX': {
        split: {
            start: 0,
            length: 2,
            to: 'SETNX'
        }
    },

    'MULTI': {
        handler: function(cid, prm){
            global['conn'][cid].isMulti = true;
            global['conn'][cid].socket.write('+OK\r\n');
        }
    },
    'EXEC': {
        handler: function(cid, prm){
            // Count of queued commands.
            global['conn'][cid].socket.write('*' + global['conn'][cid].multiQueue.length + '\r\n');

            // Process queued commands.
            for(var x in global['conn'][cid].multiQueue){
                x = global['conn'][cid].multiQueue[x];
                
                // Execute!
                worker.work({instr: 'command', cmd: x.cmd, prm: x.prm, connectionId: cid, dequeuing: true, defer_id: defer_count++});
            }
            global['conn'][cid].isMulti = false;
        }
    },
    'SUBSCRIBE': {
        handler: function(cid, prm){
            var key = prm[0];
            var host = ring.getNode(key);
            
            global['conn'][cid]['redis'][host].subscribe(key);
            global['conn'][cid]['channels']++;

            //global['conn'][cid].socket.write(':1\r\n');
        },
        split: {
            start: 0,
            length: 1
        }
    },
    'UNSUBSCRIBE': {
        handler: function(cid, prm){
            var key = prm[0];
            var host = ring.getNode(key);

            global['conn'][cid]['redis'][host].unsubscribe(key);
            global['conn'][cid]['channels']--;

            //global['conn'][cid].socket.write(':1\r\n');
        },
        split: {
            start: 0,
            length: 1
        }
    },
    'PUBLISH': {
        handler: function(cid, prm){
            var key = prm[0];
            var val = prm[1];
            var host = ring.getNode(key);

            global['conn'][cid]['redis'][host].publish(key, val, function(name, subscribedCount){
                global['conn'][cid].socket.write(':' + subscribedCount + '\r\n');
                
            });

        },
        split: {
            start: 1,
            length: 1
        }
    }
    
};

// Get command attributes.
function redisGetAttribute(cmd){
    cmd = cmd.toString().toUpperCase();
    var rst = JSON.parse(JSON.stringify(redisDefaultCommandAttribute)); // Tricky object deep copy.
    for(var x in redisSpecialCommandAttribute[cmd]) {
        rst[x] = redisSpecialCommandAttribute[cmd][x];
    }
    return rst;
}

function redisEncode(res, attr, callback){
    var rst = "";


    // Response codes
    for(var x in redisResponseTypes){
        x = redisResponseTypes[x];
        if(res == x){
            callback("+" + x + "\r\n");
            return;
        }
    }
    for(var x in redisVariableTypes){
        x = redisVariableTypes[x];
        if(res == x){
            callback("+" + x + "\r\n");
            return;
        }
    }

    switch(Object.prototype.toString.call(res)){
        case '[object Null]':
            rst += '$-1\r\n';           
            break;
        case '[object Number]':
            rst += ':' + res + '\r\n';
            break;
        case '[object String]':
            rst += '$' + res.length + '\r\n' + res + '\r\n';
            break;
        case '[object Object]':
            var nObj = Object.keys(res).length;
            rst += '*' + (nObj*2) + '\r\n';
            for(var x in res){
                // Key
                rst += '$' + x.length + '\r\n';
                rst += x + '\r\n';
                // Value
                rst += '$' + res[x].length + '\r\n';
                rst += res[x] + '\r\n';
            }
            break;
        case '[object Array]':
            var nArr = res.length;
            rst += '*' + nArr + '\r\n';
            for(var x in res){
                rst += '$' + res[x].length + '\r\n';
                rst += res[x] + '\r\n';
            }
            break;
    }
    callback(rst);
}

function redisDecode(data, callback){
    var i = 0;
    var prm;
    data = data.split('\r\n');
    while(data.length > i){
        if(data[i]){
            if(data[i][0] == '*'){ // Start of commands.
                var cmd_i = 0, j = 0;
                cmd_i = parseInt(data[i++].substring(1)), prm = [];
                for(j=0;j<cmd_i;j++,i++){
                    if(data[i][0] == '$'){
                        prm.push(data[++i]);
                    }
                }
                callback(prm);
            } else {
                i++;
            }
        }
    }
}



exports.getAttribute = redisGetAttribute;
exports.encode = redisEncode;
exports.decode = redisDecode;

