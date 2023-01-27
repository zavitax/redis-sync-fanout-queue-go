local roomClientID = argClientID .. "::" .. argRoomID;

redis.call("ZREM", keyRoomSetOfKnownClients, roomClientID);
redis.call("ZREM", keyRoomSetOfAckedClients, roomClientID);
redis.call("ZREM", keyGlobalSetOfKnownClients, roomClientID);

redis.call("RPUSH", keyPubsubAdminEventsRemoveClientTopic, roomClientID);
