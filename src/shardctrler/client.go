package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId   int64 // the unique id of this clerk.
	commandId int64 // the command id to allocate for an op.
	leaderId  int64 // known leader, defaults to the servers[0].
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

// Join sends a Join command to the shard controller.
func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{Op: Join, Servers: servers}
	ck.Command(args)
}

// Leave sends a Leave command to the shard controller.
func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{Op: Leave, GIDs: gids}
	ck.Command(args)
}

// Move sends a Move command to the shard controller.
func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{Op: Move, Shard: shard, GID: gid}
	ck.Command(args)
}

// Query sends a Query command to the shard controller and returns the configuration.
func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{Op: Query, Num: num}
	return ck.Command(args)
}

// Command sends a command to the shard controller.
// It tries to send the command to the current leader.
// If the leader is wrong or the call times out,
// it tries the next server in the list as the leader.
// If the command is successfully sent, it increments the command ID and returns the configuration in the reply.
func (ck *Clerk) Command(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clerkId, ck.commandId
	for {
		var reply CommandReply
		// If the call fails due to wrong leader or timeout, try the next server as leader.
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		} else {
			ck.commandId++
			return reply.Config
		}
	}
}
