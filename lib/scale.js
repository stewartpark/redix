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
    Scale-out utility.
*/

var path = require('path'),
    protocol = require(path.join(__dirname, 'protocol'));

function redisSplitIfPossible(cid, cmd, prm, attr, callback, lastCallback){
    var conn = global['conn'];
    if(attr['split']){
        var _start = attr['split']['start'];
        var _length = attr['split']['length'];
        var _to = attr['split']['to']; //.toString().toUpperCase();
        var totalCount; 
        // Default
        if(!_start) _start = 0;
        if(!_length) _length = 1;
        if(!_to) _to = cmd;
        
        _to = _to.toString().toUpperCase();

        totalCount = (prm.length - _start) / _length;

        // Total
		/*	if(!conn[cid]['isMulti']){
			conn[cid].socket.write('*' + totalCount + '\r\n');
		}*/

        //t_defer_count = parseInt(global['defer_count']);
        //global['defer_count'] += totalCount;

        for(var i = _start;i < prm.length;i+=_length) {
            var new_prm = [];

            for(var j = 0; j < _start; j++){
                new_prm.push(prm[j]);
            }
            for(var j = 0; j < _length; j++) {
                new_prm.push(prm[i+j]);
            }

            // Generate new defer_id ticket which doesn't affect defer_ok.
            callback(/*t_defer_count++,*/ _to, new_prm, protocol.getAttribute(_to));
        }

        //global['defer_ok'] ++;
		if(lastCallback) lastCallback();
        return true;
    } else {
        return false;
    }
}

function redisNumberOfSplitable(cid, cmd, prm, attr){
    var conn = global['conn'];
    if(attr['split']){
        var _start = attr['split']['start'];
        var _length = attr['split']['length'];
        var _to = attr['split']['to']; //.toString().toUpperCase();
        var totalCount; 

        // Default
        if(!_start) _start = 0;
        if(!_length) _length = 1;
        
        totalCount = (prm.length - _start) / _length;

		return totalCount;
    } else {
		return 1;
    }
}



exports.redisSplitIfPossible = redisSplitIfPossible;
exports.redisNumberOfSplitable = redisNumberOfSplitable;
