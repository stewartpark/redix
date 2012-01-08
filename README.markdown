Redix
======
Redix is ***a multiplexed redis proxy server*** written in nodeJS.

This aims high scalability.


## Quick start

Copy `example/redix.cfg` to `/etc`, and edit it as follows,

    192.168.0.1 6379
    192.168.0.2 6379

The above configuration combines two servers.

Then, just type the following command.

    node redix.js


