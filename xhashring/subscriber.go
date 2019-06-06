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
)

const (
	MT_HashRingFlags = 0x20000 | iota
)

const (
	HRF_Member = 1<<iota
)

type Subscriber struct{
	Tab *Table
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

func (s *Subscriber) Attach(wn *mlst.WrapNode) {
	wn.Deleg.AsyncHooks = append(wn.Deleg.AsyncHooks,s)
}

