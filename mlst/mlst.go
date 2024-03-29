/*
Copyright (c) 2019 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package mlst

import (
	"bytes"
	xdr "github.com/davecgh/go-xdr/xdr2"
	"github.com/hashicorp/memberlist"
	"github.com/byte-mug/golibs/concurrent/sortlist"
	"github.com/emirpasic/gods/utils"
	"github.com/byte-mug/golibs/bufferex"
)

type NodeMeta map[uint32]uint32
func (n NodeMeta) Bytes() []byte {
	buf := new(bytes.Buffer)
	xdr.Marshal(buf,n)
	return buf.Bytes()
}
func (n NodeMeta) Has(k uint32) (ok bool) {
	_,ok = n[k]
	return
}
func (n NodeMeta) HasFlags(k, f uint32) (ok bool) {
	var g uint32
	g,ok = n[k]
	return (g&f)!=0
}

func DecodeNodeMeta(m []byte) (n NodeMeta) {
	xdr.Unmarshal(bytes.NewReader(m), &n)
	return
}

type InternalNode struct {
	Metadata []byte
	Tlq memberlist.TransmitLimitedQueue
	Msg chan bufferex.Binary
	Nodes sortlist.Sortlist
	AsyncHooks []memberlist.EventDelegate
	SyncHooks []memberlist.EventDelegate
}
//func (i *InternalNode)
func (i *InternalNode) nodes() int {
	return 1
}
func (i *InternalNode) Initialize() {
	i.Tlq.NumNodes,i.Tlq.RetransmitMult = i.nodes,1
	i.Msg = make(chan bufferex.Binary,64)
	i.Nodes.Cmp = utils.StringComparator
}

func (i *InternalNode) NodeMeta(limit int) []byte {
	if limit<len(i.Metadata) { return nil }
	return i.Metadata
}

func (i *InternalNode) ConsumeB(v bufferex.Binary) {
	i.Msg <- v
}
func (i *InternalNode) ConsumeNB(v bufferex.Binary) {
	select {
	case i.Msg <- v:
	default: v.Free()
	}
}
func (i *InternalNode) NotifyMsg(b []byte) {
	v := bufferex.NewBinary(b)
	i.ConsumeNB(v)
}

// This impl. just opportunistically calls into Tlq
func (i *InternalNode) GetBroadcasts(overhead, limit int) [][]byte {
	if i.Tlq.NumNodes==nil { return nil }
	return i.Tlq.GetBroadcasts(overhead,limit)
}
func (i *InternalNode) LocalState(join bool) []byte { return nil }
func (i *InternalNode) MergeRemoteState(buf []byte, join bool) { }

var _ memberlist.Delegate = (*InternalNode)(nil)


func (i *InternalNode) NotifyJoin(node *memberlist.Node) {
	i.Nodes.Insert(node.Name,node)
	for _,h := range i.AsyncHooks { go h.NotifyJoin(node) }
	for _,h := range i.SyncHooks { h.NotifyJoin(node) }
}

func (i *InternalNode) NotifyUpdate(node *memberlist.Node) {
	for _,h := range i.AsyncHooks { go h.NotifyUpdate(node) }
	for _,h := range i.SyncHooks { h.NotifyUpdate(node) }
}

func (i *InternalNode) NotifyLeave(node *memberlist.Node) {
	defer i.Nodes.Delete(node.Name)
	for _,h := range i.AsyncHooks { go h.NotifyLeave(node) }
	for _,h := range i.SyncHooks { h.NotifyLeave(node) }
}

var _ memberlist.EventDelegate = (*InternalNode)(nil)

