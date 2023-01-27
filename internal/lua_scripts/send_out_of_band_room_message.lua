local res = redis.call("RPUSH", keyRoomPubsub, argMsg);

return res;
