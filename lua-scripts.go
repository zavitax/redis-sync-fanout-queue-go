package redisSyncFanoutQueue

import (
	"embed"
	"fmt"

	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

var (
	scriptCreateClientID                 *redisLuaScriptUtils.RedisScript
	scriptUpdateClientTimestamp          *redisLuaScriptUtils.RedisScript
	scriptAddSyncClientToRoom            *redisLuaScriptUtils.RedisScript
	scriptRemoveSyncClientFromRoom       *redisLuaScriptUtils.RedisScript
	scriptSendOutOfBandRoomMessage       *redisLuaScriptUtils.RedisScript
	scriptConditionalProcessRoomMessages *redisLuaScriptUtils.RedisScript
	scriptEnqueueRoomMessage             *redisLuaScriptUtils.RedisScript
	scriptAckClientMessage               *redisLuaScriptUtils.RedisScript
	scriptGetMetrics                     *redisLuaScriptUtils.RedisScript
)

//go:embed "internal/lua_scripts"
var scriptDir embed.FS

func init() {
	luaFiles := []string{
		"create_client_id.lua",
		"update_client_timestamp.lua",
		"add_sync_client_to_room.lua",
		"remove_sync_client_from_room.lua",
		"send_out_of_band_room_message.lua",
		"conditional_process_room_messages.lua",
		"enqueue_room_message.lua",
		"ack_client_message.lua",
		"get_metrics.lua",
	}
	luaFileMap := make(map[string]string, len(luaFiles))

	for _, filename := range luaFiles {
		file, err := scriptDir.ReadFile(fmt.Sprintf("internal/lua_scripts/%s", filename))
		if err != nil {
			panic(err)
		}
		luaFileMap[filename] = string(file)
	}

	scriptCreateClientID = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyClientIDSequence", "keyLastTimestamp"},
		[]string{"argCurrentTimestamp"},
		luaFileMap["create_client_id.lua"],
	)

	scriptUpdateClientTimestamp = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients"},
		[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
		luaFileMap["update_client_timestamp.lua"],
	)

	scriptAddSyncClientToRoom = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients"},
		[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
		luaFileMap["add_sync_client_to_room.lua"],
	)

	scriptRemoveSyncClientFromRoom = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyGlobalSetOfKnownClients", "keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyPubsubAdminEventsRemoveClientTopic"},
		[]string{"argClientID", "argRoomID", "argCurrentTimestamp"},
		luaFileMap["remove_sync_client_from_room.lua"],
	)

	scriptSendOutOfBandRoomMessage = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyRoomPubsub"},
		[]string{"argRoomID", "argMsg"},
		luaFileMap["send_out_of_band_room_message.lua"],
	)

	scriptConditionalProcessRoomMessages = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyRoomPubsub"},
		[]string{"argRoomID"},
		luaFileMap["conditional_process_room_messages.lua"],
	)

	scriptEnqueueRoomMessage = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyRoomSetOfKnownClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyGlobalAckedRooms"},
		[]string{"argRoomID", "argPriority", "argMsg"},
		luaFileMap["enqueue_room_message.lua"],
	)

	scriptAckClientMessage = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyRoomSetOfKnownClients", "keyRoomSetOfAckedClients", "keyGlobalKnownRooms", "keyRoomQueue", "keyRoomPubsub"},
		[]string{"argRoomID", "argClientID", "argCurrentTimestamp"},
		luaFileMap["ack_client_message.lua"],
	)

	scriptGetMetrics = redisLuaScriptUtils.NewRedisScript(
		[]string{"keyGlobalKnownRooms", "keyGlobalSetOfKnownClients"},
		[]string{"argTopRoomsLimit"},
		luaFileMap["get_metrics.lua"],
	)
}
