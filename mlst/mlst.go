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
	"crypto/md5"
	xdr "github.com/davecgh/go-xdr/xdr2"
	"github.com/hashicorp/memberlist"
	"github.com/byte-mug/golibs/chordhash"
	avl "github.com/emirpasic/gods/trees/avltree"
	"github.com/byte-mug/golibs/bufferex"
)

const (
	NM_IsDHT = 0x10000 | iota
)

type NodeMeta map[uint32]uint32
func (n NodeMeta) Bytes() []byte {
	buf := new(bytes.Buffer)
	xdr.Marshal(buf,n)
	return buf.Bytes()
}
func (n NodeMeta) Has(u uint32) (ok bool) {
	_,ok = n[u]
	return
}

func DecodeNodeMeta(m []byte) (n NodeMeta) {
	xdr.Unmarshal(bytes.NewReader(m), &n)
	return
}

type InternalNode struct {
	Metadata []byte
	Tlq memberlist.TransmitLimitedQueue
	Msg chan bufferex.Binary
	nm chordhash.NodeManager
	nd *avl.Tree
}
//func (i *InternalNode)
func (i *InternalNode) nodes() int {
	return 1
}
func (i *InternalNode) Initialize() {
	i.Tlq.NumNodes,i.Tlq.RetransmitMult = i.nodes,1
	i.nm.Init()
	i.nd = avl.NewWithStringComparator()
	i.Msg = make(chan bufferex.Binary,64)
}
func (i *InternalNode) SetNodeName(s string) {
	sum := md5.Sum([]byte(s))
	i.nm.SetSelf(sum[:])
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
func (i *InternalNode) MergeRemoteState(buf []byte, join bool) {  }

var _ memberlist.Delegate = (*InternalNode)(nil)


func (i *InternalNode) NotifyJoin(node *memberlist.Node) {
	i.nd.Put(node.Name,node)
	
	n := DecodeNodeMeta(node.Meta)
	if n.Has(NM_IsDHT) {
		h := md5.Sum([]byte(node.Name))
		i.nm.Insert(h[:],node)
	}
}

func (i *InternalNode) NotifyUpdate(node *memberlist.Node) {
	//i.nd.Put(node.Name,node)
}

func (i *InternalNode) NotifyLeave(node *memberlist.Node) {
	i.nd.Remove(node.Name)
	
	n := DecodeNodeMeta(node.Meta)
	if n.Has(NM_IsDHT) {
		h := md5.Sum([]byte(node.Name))
		i.nm.Remove(h[:])
	}
}

var _ memberlist.EventDelegate = (*InternalNode)(nil)



