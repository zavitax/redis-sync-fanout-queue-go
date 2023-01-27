local roomClientID = argClientID .. "::" .. argRoomID;

local clientExistsInGlobal = tonumber(redis.call("ZRANK", keyGlobalSetOfKnownClients, roomClientID));

if (clientExistsInGlobal ~= nil) then
  redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
  redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);

  return 1;
else
  return 0;
end
