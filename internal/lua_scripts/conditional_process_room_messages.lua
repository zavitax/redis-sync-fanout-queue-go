local knownClients = tonumber(redis.call("ZCARD", keyRoomSetOfKnownClients));

if (knownClients > 0) then
  local ackedClients = tonumber(redis.call("ZCARD", keyRoomSetOfAckedClients));
  
  if (ackedClients > 0 and ackedClients == knownClients) then
    -- Remove next message from queue
    local msgs = redis.call("ZPOPMIN", keyRoomQueue, 1);
  
    if (#msgs > 0) then
      -- All known clients ACKed
      redis.call("DEL", keyRoomSetOfAckedClients); -- Remove ACKed clients

      -- Publish message to PUBSUB listeners
      local res = redis.call("RPUSH", keyRoomPubsub, msgs[1]);
    end
  end
else
  -- No clients subscribe for sync delivery
  
  -- Get all remaining messages
  local messages = redis.call("ZRANGEBYSCORE", keyRoomQueue, "-inf", "+inf");

  for i, msg in ipairs(messages) do
    -- Publish message to PUBSUB listeners
    redis.call("RPUSH", keyRoomPubsub, msg);
  end

  -- Clear queue
  redis.call("DEL", keyRoomQueue);
end

local remainingMsgCount = tonumber(redis.call("ZCARD", keyRoomQueue))

if (remainingMsgCount > 0) then
  redis.call("ZADD", keyGlobalKnownRooms, "CH", remainingMsgCount, argRoomID);
else
  redis.call("ZREM", keyGlobalKnownRooms, argRoomID);
end

return 1;
