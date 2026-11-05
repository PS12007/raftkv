package linearize

import (
	"fmt"
	"sort"
	"strings"
)

// KvInput is the input of a KV operation.
type KvInput struct {
	Op    uint8 // 0 get, 1 put, 2 append, 3 delete (matches kv.OpType)
	Key   string
	Value string
}

// KvOutput is the output of a KV operation.
type KvOutput struct {
	Value string
}

// KvModel is the sequential specification of the KV store. Missing keys read
// as "". Histories are partitioned by key.
var KvModel = Model{
	Partition: func(h []Operation) [][]Operation {
		byKey := map[string][]Operation{}
		for _, op := range h {
			k := op.Input.(KvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		in := input.(KvInput)
		st := state.(string)
		switch in.Op {
		case 0:
			out, ok := output.(KvOutput)
			return ok && out.Value == st, st
		case 1:
			return true, in.Value
		case 2:
			return true, st + in.Value
		case 3:
			return true, ""
		}
		return false, st
	},
	DescribeOperation: func(input, output any) string {
		in := input.(KvInput)
		switch in.Op {
		case 0:
			if out, ok := output.(KvOutput); ok {
				return fmt.Sprintf("get(%q) -> %q", in.Key, out.Value)
			}
			return fmt.Sprintf("get(%q) -> ?", in.Key)
		case 1:
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		case 2:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		case 3:
			return fmt.Sprintf("delete(%q)", in.Key)
		}
		return "?"
	},
}

// Describe renders a sub-history (e.g. Info.FailedPartition) as text, one
// operation per line, sorted by call time.
func Describe(model Model, ops []Operation, longest []int) string {
	idx := make([]int, len(ops))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return ops[idx[a]].Call < ops[idx[b]].Call })
	var sb strings.Builder
	for _, i := range idx {
		op := ops[i]
		ret := fmt.Sprint(op.Return)
		if op.Return == Pending {
			ret = "pending"
		}
		d := fmt.Sprint(op.Input, op.Output)
		if model.DescribeOperation != nil {
			d = model.DescribeOperation(op.Input, op.Output)
		}
		fmt.Fprintf(&sb, "client %-3d [%d, %s] %s\n", op.ClientID, op.Call, ret, d)
	}
	if len(longest) > 0 {
		sb.WriteString("longest linearizable prefix:\n")
		for _, i := range longest {
			op := ops[i]
			d := fmt.Sprint(op.Input, op.Output)
			if model.DescribeOperation != nil {
				d = model.DescribeOperation(op.Input, op.Output)
			}
			fmt.Fprintf(&sb, "  %s\n", d)
		}
	}
	return sb.String()
}
