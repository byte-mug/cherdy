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


package xhashring

import (
	"github.com/byte-mug/cherdy/mlst"
	"github.com/hashicorp/memberlist"
	"github.com/byte-mug/golibs/bufferex"
	avl "github.com/emirpasic/gods/trees/avltree"
)

const (
	MT_HashRingFlags = 0x20000 | iota
)

const (
	HRF_Subscriber = 1<<iota
	HRF_Member
)

const (
	// TODO: use a low number (base 0).
	MH_HrRoute = 0x20000 + iota
)

/*
Route-Flags
*/
const (
	RTF_Last = 1<<iota
)

type Subscriber struct{
	Tab *Table
	Num int // Replication Factor
	Node *mlst.WrapNode
}

func (s *Subscriber) NotifyJoin(node *memberlist.Node) {
	m := mlst.DecodeNodeMeta(node.Meta)
	if m.HasFlags(MT_HashRingFlags,HRF_Member) {
		s.Tab.Join(node.Name)
	} else {
		s.Tab.Invalidate(node.Name)
	}
}

func (s *Subscriber) NotifyUpdate(node *memberlist.Node) {
	m := mlst.DecodeNodeMeta(node.Meta)
	if m.HasFlags(MT_HashRingFlags,HRF_Member) {
		s.Tab.Validate(node.Name)
	} else {
		s.Tab.Invalidate(node.Name)
	}
}

func (s *Subscriber) NotifyLeave(node *memberlist.Node) {
	s.Tab.Leave(node.Name)
}

func (s *Subscriber) CheckSelf(head *avl.Node) bool {
	v := head
	i := s.Num
	for i>0 {
		if s.Node.Name == v.Value.(*Entry).Name { return true }
		v = s.Tab.Step(v)
		i--
	}
	return false
}
func (s *Subscriber) findFirst(head *avl.Node) (n *memberlist.Node) {
	v := head
	i := s.Num
	for i>0 {
		n = s.Node.Lookup(v.Value.(*Entry).Name)
		if n!=nil { break }
		v = s.Tab.Step(v)
		i--
	}
	return
}
func (s *Subscriber) findLast(head *avl.Node) (n *memberlist.Node) {
	v := head
	i := s.Num
	for i>0 {
		m := s.Node.Lookup(v.Value.(*Entry).Name)
		if m!=nil { n = m }
		v = s.Tab.Step(v)
		i--
	}
	return
}


func (s *Subscriber) HrRoute(w *mlst.WrapNode, d *mlst.MessageReader, msg bufferex.Binary) bool {
	flag,err := d.DecodeInt()
	if err!=nil { return false }
	
	id,err := d.DecodeString()
	if err!=nil { return false }
	
	v := s.Tab.Next(id)
	
	// If there are no routable nodes, discard.
	if v==nil { return false }
	
	// If this packet is for us, consume the rest of it.
	if s.CheckSelf(v) { return true }
	
	var node *memberlist.Node
	
	switch {
	case (flag&RTF_Last)!=0:
		node = s.findLast(v)
	default:
		node = s.findFirst(v)
	}
	
	// If ther is no alive node, we cannot forward this message.
	if node==nil { return false }
	
	mb := new(mlst.MessageBuffer).Init()
	
	// Rewrite the header.
	mb.EncodeMulti(MH_HrRoute,flag,id)
	
	// Append the rest of the packet.
	d.WriteTo(mb)
	
	// Forward the packet to the other node.
	s.Node.SendTo(mlst.ST_BestFit,node,mb.Bytes())
	
	return false
}

func (s *Subscriber) Attach(wn *mlst.WrapNode) {
	s.Node = wn
	wn.Deleg.AsyncHooks = append(wn.Deleg.AsyncHooks,s)
	wn.Handlers[MH_HrRoute] = s.HrRoute
	wn.Meta[MT_HashRingFlags] |= HRF_Subscriber
}

func BecomeMember(wn *mlst.WrapNode) {
	wn.Meta[MT_HashRingFlags] |= HRF_Member
}
