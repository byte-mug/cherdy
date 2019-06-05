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
	"github.com/vmihailenco/msgpack"
	"github.com/hashicorp/memberlist"
	//"github.com/byte-mug/golibs/chordhash"
	//avl "github.com/emirpasic/gods/trees/avltree"
	"github.com/byte-mug/golibs/bufferex"
)

const (
	MH_ForwardFinger uint64 = iota
	MH_ForwardPrecise
)

func ReadMessage(b []byte) *msgpack.Decoder {
	dec := msgpack.NewDecoder(bytes.NewReader(b))
	dec.UseDecodeInterfaceLoose(true)
	return dec
}
type MessageBuffer struct{
	*msgpack.Encoder
	buf bytes.Buffer
}
func (m *MessageBuffer) Bytes() []byte { return m.buf.Bytes() }
func (m *MessageBuffer) Init() *MessageBuffer {
	m.buf.Truncate(0)
	m.Encoder = msgpack.NewEncoder(&m.buf)
	return m
}
func (m *MessageBuffer) Reset() *MessageBuffer {
	m.buf.Truncate(0)
	if m.Encoder==nil {
		m.Encoder = msgpack.NewEncoder(&m.buf)
	}
	return m
}


type Handler func(w *WrapNode,d *msgpack.Decoder, b []byte) bool

type WrapNode struct{
	Deleg InternalNode
	Membl *memberlist.Memberlist
	Handlers map[uint64]Handler
}

// func (w *WrapNode)

func forwardFinger(w *WrapNode,d *msgpack.Decoder, b []byte) bool {
	nid,err := d.DecodeBytes()
	if err!=nil { return false }
	sn := w.Deleg.nm.LookupFinger(nid)
	if sn==nil { return false }
	
	/* Process further. */
	if w.Deleg.nm.Self == sn.Key.(string) { return true }
	
	w.Send(sn.Value.(*memberlist.Node),b)
	
	return false
}
func forwardPrecise(w *WrapNode,d *msgpack.Decoder, b []byte) bool {
	nid,err := d.DecodeBytes()
	if err!=nil { return false }
	sn := w.Deleg.nm.LookupPrecise(nid)
	if sn==nil { return false }
	
	/* Process further. */
	if w.Deleg.nm.Self == sn.Key.(string) { return true }
	
	w.Send(sn.Value.(*memberlist.Node),b)
	
	return false
}


func (w *WrapNode) Initialize() {
	w.Deleg.Initialize()
	w.Handlers = make(map[uint64]Handler)
	w.Handlers[MH_ForwardFinger ] = forwardFinger
	w.Handlers[MH_ForwardPrecise] = forwardPrecise
}

/*
Called, after the memberlist has been created.
*/
func (w *WrapNode) Start() {
	go w.consumer()
}

func (w *WrapNode) consume(msg bufferex.Binary) {
	defer msg.Free()
	dec := ReadMessage(msg.Bytes())
restart:
	i,e := dec.DecodeUint64()
	if e!=nil { return }
	h := w.Handlers[i]
	if h==nil { return }
	if h(w,dec,msg.Bytes()) { goto restart }
}

func (w *WrapNode) consumer() {
	for msg := range w.Deleg.Msg {
		w.consume(msg)
	}
}
func (w *WrapNode) Send(to *memberlist.Node, msg []byte) error {
	if len(msg)<=912 {
		return w.Membl.SendBestEffort(to,msg)
	}
	return w.Membl.SendReliable(to,msg)
}


//
