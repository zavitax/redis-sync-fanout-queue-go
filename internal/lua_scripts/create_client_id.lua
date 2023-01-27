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
