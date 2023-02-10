local messagesCount = redis.call("ZCARD", keyRoomQueue);
local knownClients = redis.call("ZRANGE", keyRoomSetOfKnownClients, 0, -1, "WITHSCORES");
local ackedClients = redis.call("ZRANGE", keyRoomSetOfAckedClients, 0, -1, "WITHSCORES");

return {
    argRoomID,
    messagesCount,
    knownClients,
    ackedClients
}
