package redisSyncFanoutQueue

var scriptCreateClientID = `
  local keyClientIDSequence = KEYS[1];
  local keyLastTimestamp = KEYS[2];

  local argCurrentTimestamp = tonumber(ARGV[1]);

  local lastTimestamp = tonumber(redis.call("GET", keyLastTimestamp));
  local seq = 0;

  if (lastTimestamp == argCurrentTimestamp) then
    seq = redis.call("INCR", keyClientIDSequence);
  else
    redis.call("SET", keyClientIDSequence, seq);
    redis.call("SET", keyLastTimestamp, argCurrentTimestamp);

    lastTimestamp = argCurrentTimestamp;
  end

  return lastTimestamp .. "-" .. seq;
`;

var scriptUpdateClientTimestamp = `
  local keyGlobalSetOfKnownClients = KEYS[1];
  local keyRoomSetOfKnownClients = KEYS[2];

  local argClientID = ARGV[1];
  local argRoomID = ARGV[2];
  local argCurrentTimestamp = tonumber(ARGV[3]);

  local roomClientID = argClientID .. "::" .. argRoomID;

  local clientExistsInGlobal = tonumber(redis.call("ZRANK", keyGlobalSetOfKnownClients, roomClientID));

  if (clientExistsInGlobal ~= nil) then
    redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
    redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
  end
`;

var scriptAddSyncClientToRoom = `
  local keyGlobalSetOfKnownClients = KEYS[1];
  local keyRoomSetOfKnownClients = KEYS[2];
  local keyRoomSetOfAckedClients = KEYS[3];

  local argClientID = ARGV[1];
  local argRoomID = ARGV[2];
  local argCurrentTimestamp = tonumber(ARGV[3]);

  local roomClientID = argClientID .. "::" .. argRoomID;

  local clientExistsInRoom = tonumber(redis.call("ZRANK", keyRoomSetOfKnownClients, roomClientID));

  if (clientExistsInRoom == nil) then
    redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
    redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
    
    redis.call("ZADD", keyRoomSetOfAckedClients, "CH", argCurrentTimestamp, roomClientID);
  else
    return "ERR_CLIENT_ID_EXISTS_IN_ROOM";
  end
`;

var scriptRemoveSyncClientFromRoom = `
  local keyGlobalSetOfKnownClients = KEYS[1];
  local keyRoomSetOfKnownClients = KEYS[2];
  local keyRoomSetOfAckedClients = KEYS[3];
  local keyPubsubAdminEventsRemoveClientTopic = KEYS[4];

  local argClientID = ARGV[1];
  local argRoomID = ARGV[2];
  local argCurrentTimestamp = tonumber(ARGV[3]);

  local roomClientID = argClientID .. "::" .. argRoomID;

  redis.call("ZREM", keyRoomSetOfKnownClients, roomClientID);
  redis.call("ZREM", keyRoomSetOfAckedClients, roomClientID);
  redis.call("ZREM", keyGlobalSetOfKnownClients, roomClientID);

  redis.call("PUBLISH", keyPubsubAdminEventsRemoveClientTopic, roomClientID);
`;

/*
var scriptRemoveTimedOutClients = `
  local keyGlobalSetOfKnownClients = KEYS[1];
  local keyRoomSetOfKnownClients = KEYS[2];
  local keyRoomSetOfAckedClients = KEYS[3];
  local keyPubsubAdminEventsRemoveClientTopic = KEYS[4];

  local argMaxTimestampToRemove = tonumber(ARGV[1]);

  local timedOutRoomClientIDs = redis.call("ZRANGEBYSCORE", keyGlobalSetOfKnownClients, "-inf", argMaxTimestampToRemove);

  for i, roomClientID in ipairs(timedOutRoomClientIDs) do
    redis.call("ZREM", keyRoomSetOfKnownClients, roomClientID);
    redis.call("ZREM", keyRoomSetOfAckedClients, roomClientID);
    redis.call("ZREM", keyGlobalSetOfKnownClients, roomClientID);

    redis.call("PUBLISH", keyPubsubAdminEventsRemoveClientTopic, roomClientID);
  end

  return timedOutRoomClientIDs;
`;*/

var scriptConditionalProcessRoomMessages = `
  local keyRoomSetOfKnownClients = KEYS[1];
  local keyRoomSetOfAckedClients = KEYS[2];
  local keyGlobalKnownRooms = KEYS[3];
  local keyRoomQueue = KEYS[4];
  local keyRoomPubsub = KEYS[5];

  local argRoomID = ARGV[1];

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
        redis.call("PUBLISH", keyRoomPubsub, msgs[1]);
      end
    end
  else
    -- No clients subscribe for sync delivery
    
    -- Get all remaining messages
    local messages = redis.call("ZRANGEBYSCORE", keyRoomQueue, "-inf", "+inf");
    
    for i, msg in ipairs(messages) do
      -- Publish message to PUBSUB listeners
      redis.call("PUBLISH", keyRoomPubsub, msg);
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
`;

var scriptEnqueueRoomMessage = `
  local keyRoomSetOfKnownClients = KEYS[1];
  local keyGlobalKnownRooms = KEYS[2];
  local keyRoomQueue = KEYS[3];

  local argRoomID = ARGV[1];
  local argPriority = tonumber(ARGV[2]);
  local argMsg = ARGV[3];

  redis.call("ZADD", keyRoomQueue, argPriority, argMsg);
  redis.call("ZINCRBY", keyGlobalKnownRooms, 1, argRoomID);
`;

var scriptAckClientMessage = `
  local keyRoomSetOfKnownClients = KEYS[1];
  local keyRoomSetOfAckedClients = KEYS[2];
  local keyGlobalKnownRooms = KEYS[3];
  local keyRoomQueue = KEYS[4];
  local keyRoomPubsub = KEYS[5];

  local argRoomID = ARGV[1];
  local argClientID = ARGV[2];
  local argCurrentTimestamp = tonumber(ARGV[3]);

  local roomClientID = argClientID .. "::" .. argRoomID;

  local clientExistsInRoom = tonumber(redis.call("ZRANK", keyRoomSetOfKnownClients, roomClientID));

  if (clientExistsInRoom == nil) then
    return "ERR_NO_CLIENT_ID_IN_ROOM";
  else
    redis.call("ZADD", keyRoomSetOfAckedClients, "CH", argCurrentTimestamp, roomClientID);
  end
`;

var scriptGetMetrics = `
  local keyGlobalKnownRooms = KEYS[1];
  local keyGlobalSetOfKnownClients = KEYS[2];

  local argTopRoomsLimit = tonumber(ARGV[1])

  return {
    redis.call('ZCARD', keyGlobalKnownRooms),
    redis.call('ZREVRANGE', keyGlobalKnownRooms, 0, argTopRoomsLimit, 'WITHSCORES'),
    redis.call('ZCARD', keyGlobalSetOfKnownClients)
  }
`;
