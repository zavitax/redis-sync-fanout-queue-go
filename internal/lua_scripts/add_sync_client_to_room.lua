local roomClientID = argClientID .. "::" .. argRoomID;

local clientExistsInRoom = tonumber(redis.call("ZRANK", keyRoomSetOfKnownClients, roomClientID));

if (clientExistsInRoom == nil) then
  redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
  redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
  
  redis.call("ZADD", keyRoomSetOfAckedClients, "CH", argCurrentTimestamp, roomClientID);
end

return 1
