return {
  redis.call('ZCARD', keyGlobalKnownRooms),
  redis.call('ZREVRANGE', keyGlobalKnownRooms, 0, argTopRoomsLimit, 'WITHSCORES'),
  redis.call('ZCARD', keyGlobalSetOfKnownClients)
}
