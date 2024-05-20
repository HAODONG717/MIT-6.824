package shardctrler

import (
	"fmt"
	"log"
)

const Debug = false

func (sc *ShardCtrler) printOp(op Op) string {
	return fmt.Sprintf("[op.Index:%v, op.ClientId:%v,op.SeqId:%v,op.Num:%v,op.Shard:%v,op.GID:%v,op.GIDs:%v,op.Servers:%v,op.OpType:%v]，op.Cfg:%v,: ",
		op.Index, op.ClientId, op.SeqId, op.Num, op.Shard, op.GID, op.GIDs, op.Servers, op.OpType, op.Cfg)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func ifCond(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	} else {
		return b
	}
}
func getcount() func() int {
	cnt := 1000
	return func() int {
		cnt++
		return cnt
	}
}
