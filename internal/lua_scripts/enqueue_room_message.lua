redis.call("ZADD", keyRoomQueue, argPriority, argMsg);

local remainingMsgCount = tonumber(redis.call("ZCARD", keyRoomQueue))

redis.call("ZADD", keyGlobalKnownRooms, "CH", remainingMsgCount, argRoomID);

return 1;
