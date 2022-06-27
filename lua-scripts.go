package redisSyncFanoutQueue

import (
	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

var scriptCreateClientID = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyClientIDSequence", "keyLastTimestamp"},
	[]string{"argCurrentTimestamp"},
	`
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
  `)

var scriptUpdateClientTimestamp = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients"},
	[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
	`
    local roomClientID = argClientID .. "::" .. argRoomID;

    local clientExistsInGlobal = tonumber(redis.call("ZRANK", keyGlobalSetOfKnownClients, roomClientID));

    if (clientExistsInGlobal ~= nil) then
      redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
      redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);

      return 1;
    else
      return 0;
    end
  `)

var scriptAddSyncClientToRoom = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients"},
	[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
	`
    local cp = ""
    local roomClientID = argClientID .. "::" .. argRoomID;

    local clientExistsInRoom = tonumber(redis.call("ZRANK", keyRoomSetOfKnownClients, roomClientID));

    if (clientExistsInRoom == nil) then
      cp = "ADDING_CLIENT"
      redis.call("ZADD", keyGlobalSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
      redis.call("ZADD", keyRoomSetOfKnownClients, "CH", argCurrentTimestamp, roomClientID);
      
      redis.call("ZADD", keyRoomSetOfAckedClients, "CH", argCurrentTimestamp, roomClientID);

      local currRank = tonumber(redis.call("ZRANK", keyGlobalSetOfKnownClients, roomClientID))
      local currCard = tonumber(redis.call("ZCARD", keyGlobalSetOfKnownClients))
      cp = "'ADDED_CLIENT: rank: " .. currRank .. ", card: " .. currCard .. "'";
    end

    return cp
  `)

var scriptRemoveSyncClientFromRoom = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyPubsubAdminEventsRemoveClientTopic"},
	[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
	`
    local roomClientID = argClientID .. "::" .. argRoomID;

    redis.call("ZREM", keyRoomSetOfKnownClients, roomClientID);
    redis.call("ZREM", keyRoomSetOfAckedClients, roomClientID);
    redis.call("ZREM", keyGlobalSetOfKnownClients, roomClientID);

    redis.call("PUBLISH", keyPubsubAdminEventsRemoveClientTopic, roomClientID);
  `)

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

var scriptConditionalProcessRoomMessages = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyRoomPubsub"},
	[]string{"argRoomID"},
	`
    local cp = 0
    local knownClients = tonumber(redis.call("ZCARD", keyRoomSetOfKnownClients));

    cp = "'known " .. knownClients .. " (" .. keyRoomSetOfKnownClients ..")'"

    if (knownClients > 0) then
      local ackedClients = tonumber(redis.call("ZCARD", keyRoomSetOfAckedClients));
      
      cp = cp .. " -> 'known " .. knownClients .. ", acked: " .. ackedClients .. "'"
      if (ackedClients > 0 and ackedClients == knownClients) then
        -- Remove next message from queue
        local msgs = redis.call("ZPOPMIN", keyRoomQueue, 1);
      
        if (#msgs > 0) then
          -- All known clients ACKed
          redis.call("DEL", keyRoomSetOfAckedClients); -- Remove ACKed clients

          -- Publish message to PUBSUB listeners
          local res = redis.call("PUBLISH", keyRoomPubsub, msgs[1]);

          cp = cp .. " -> 'PUBLISHED_TO_SYNC " .. res .. "'"
        end
      end
    else
      -- No clients subscribe for sync delivery
      
      -- Get all remaining messages
      local messages = redis.call("ZRANGEBYSCORE", keyRoomQueue, "-inf", "+inf");

      cp = cp .. " -> 'NO_SYNC_CLIENTS msgCount: " .. #messages .. "'"

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

    return cp;
  `)

var scriptEnqueueRoomMessage = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyRoomSetOfKnownClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyGlobalAckedRooms"},
	[]string{"argRoomID", "argPriority", "argMsg"},
	`
    redis.call("ZADD", keyRoomQueue, argPriority, argMsg);
    redis.call("ZINCRBY", keyGlobalKnownRooms, 1, argRoomID);

    local remainingMsgCount = tonumber(redis.call("ZCARD", keyRoomQueue))

    redis.call("ZADD", keyGlobalKnownRooms, "CH", remainingMsgCount, argRoomID);

    return "ENQUEUE:" .. argRoomID;
  `)

var scriptAckClientMessage = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyRoomPubsub"},
	[]string{"argRoomID", "argClientID", "argCurrentTimestamp"},
	`
    local roomClientID = argClientID .. "::" .. argRoomID;

    local clientExistsInRoom = tonumber(redis.call("ZRANK", keyRoomSetOfKnownClients, roomClientID));

    if (clientExistsInRoom ~= nil) then
      redis.call("ZADD", keyRoomSetOfAckedClients, "CH", argCurrentTimestamp, roomClientID);
    end
  `)

var scriptGetMetrics = redisLuaScriptUtils.NewRedisScript(
	[]string{"keyGlobalKnownRooms", "keyGlobalSetOfKnownClients"},
	[]string{"argTopRoomsLimit"},
	`
    return {
      redis.call('ZCARD', keyGlobalKnownRooms),
      redis.call('ZREVRANGE', keyGlobalKnownRooms, 0, argTopRoomsLimit, 'WITHSCORES'),
      redis.call('ZCARD', keyGlobalSetOfKnownClients)
    }
  `)
