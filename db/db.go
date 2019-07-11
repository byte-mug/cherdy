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


package db


import (
	"github.com/dgraph-io/badger"
	"github.com/byte-mug/cherdy/mlst"
	"github.com/byte-mug/golibs/bufferex"
	
	gerrors "errors"
	perrors "github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
	"sync"
)

const (
	MH_Get = 0x10000 + iota
	MH_GetResponse
	MH_Put
	MH_PutResponse
)

const (
	RESP_OK = iota
	RESP_NotFound
	RESP_IoError
	RESP_DeadTargetNode
	
	RESP_Illegal
)

const (
	META_Raw = iota
	META_InnerRedirect
	META_OuterRedirect
)

var (
	ErrLoop = gerrors.New("Redirect Loop")
	
	errNil = gerrors.New("<nil>")
)

func unwrapCause(err error) error {
	if err!=nil { return errNil }
	for {
		nerr := perrors.Cause(err)
		if nerr==nil || nerr==err { return err }
	}
	panic("unreachable")
}

type myItem struct {
	tx *badger.Txn
	*badger.Item
}
func (l myItem) Discard() { l.tx.Discard() }

func modulo(i,m int) int {
	if m==0 { return -1 }
	return i%m
}

func db_get(db *badger.DB,key []byte) (myItem,error) {
	tx := db.NewTransaction(false)
	it,err := tx.Get(key)
	if err!=nil {
		tx.Discard();
		return myItem{},err
	}
	return myItem{tx,it},nil
}

func errsetter(wg *sync.WaitGroup, pe *error) func(error) {
	if pe==nil { return func(error) { wg.Done() } }
	return func(e error) {
		if e!=nil { *pe = e }
		wg.Done()
	}
}
func db_put(db *badger.DB,ent *badger.Entry,f func(error)) {
	tx := db.NewTransaction(true)
	defer tx.CommitWith(f)
	tx.SetEntry(ent)
}

type Freespace interface{
	Touch()
	HasFreeSpace(key,value int) bool
}
func freespacetouch(f Freespace) {
	if f!=nil { f.Touch() }
}


type Store struct {
	Redirects *badger.DB // may be nil
	Data      []*badger.DB
	
	Freespace []Freespace
}

func (s *Store) i_Get(w *mlst.WrapNode, d *mlst.MessageReader, msg bufferex.Binary) {
	defer msg.Free()
	var item myItem
	var ierr error
	hashnum,err := d.DecodeInt()
	if err!=nil { return }
	
	key,err := d.DecodeBytes()
	if err!=nil { return }
	
	usehash := hashnum
	for {
		var choice [2]*badger.DB
		
		if l := len(s.Data); l>0 { choice[0] = s.Data[usehash%l] }
		choice[1] = s.Redirects
		
		ierr = badger.ErrKeyNotFound
		for _,db := range choice {
			item,ierr = db_get(db,key)
			if ierr==nil { break }
		}
		
		if ierr!=nil { break }
		switch item.UserMeta() {
		case META_InnerRedirect:
			cnum := usehash
			ierr = item.Value(func(b []byte) error { return msgpack.Unmarshal(b,&usehash) })
			item.Discard() // Drop item/transaction
			item = myItem{}
			
			// if cnum == usehash, we use the same usehash --> loop
			if ierr==nil && cnum==usehash { ierr = ErrLoop }
			
			if ierr==nil { continue } // perform next.
		}
		if item.tx!=nil { defer item.Discard() }
		break
	}
	
	var target string
	resp := RESP_OK
	if ierr==nil {
		switch item.UserMeta() {
		case META_OuterRedirect:
			ierr = item.Value(func(b []byte) error {
				target = string(b)
				return nil
			})
			if ierr!=nil { resp = RESP_IoError; break }
			
			node := w.Lookup(target)
			
			if node==nil { resp = RESP_DeadTargetNode; break }
			
			mb := new(mlst.MessageBuffer).Init()
			
			// Rewrite the header.
			mb.EncodeMulti(MH_Get,hashnum,key)
			
			// Append the rest of the packet.
			d.WriteTo(mb)
			
			// Forward the packet to the other node.
			w.SendTo(mlst.ST_BestFit,node,mb.Bytes())
		default: break
		}
	} else if ierr==badger.ErrKeyNotFound {
		resp = RESP_NotFound
	} else {
		resp = RESP_IoError
	}
	
	target,err = d.DecodeString()
	if err!=nil { return }
	
	node := w.Lookup(target)
	if node==nil { return }
	
	targid,err := d.DecodeUint64()
	if err!=nil { return }
	
	
	mb := new(mlst.MessageBuffer).Init()
restart:
	
	// Write the header
	mb.EncodeMulti(MH_GetResponse,targid,resp)
	
	switch resp {
	case RESP_OK:
		ierr = item.Value(func(b []byte) error {
			mb.EncodeBytes(b)
			return nil
		})
		if ierr!=nil {
			resp = RESP_IoError
			mb.Reset()
			goto restart
		}
	case RESP_IoError:
		ierr = unwrapCause(ierr)
		mb.EncodeString(ierr.Error())
	}
	
	w.SendTo(mlst.ST_BestFit,node,mb.Bytes())
}
func (s *Store) Get(w *mlst.WrapNode, d *mlst.MessageReader, msg bufferex.Binary) bool {
	mlst.RetainBinary(d)
	go s.i_Get(w,d,msg)
	return false
}

func (s *Store) i_PutResponse(w *mlst.WrapNode, target string,targid uint64,resp int,add ...interface{}) {
	node := w.Lookup(target)
	if node==nil { return }
	
	mb := new(mlst.MessageBuffer).Init()
	mb.EncodeMulti(MH_GetResponse,targid,resp)
	if len(add)!=0 { mb.EncodeMulti(add...) }
	w.SendTo(mlst.ST_BestFit,node,mb.Bytes())
}
func (s *Store) i_Put(w *mlst.WrapNode, d *mlst.MessageReader, msg bufferex.Binary) {
	defer msg.Free()
	hashnum,err := d.DecodeInt()
	if err!=nil { return }
	
	meta,err := d.DecodeUint8()
	if err!=nil { return }
	
	expiresAt,err := d.DecodeUint64()
	if err!=nil { return }
	
	key,err := d.DecodeBytes()
	if err!=nil { return }
	
	value,err := d.DecodeBytes()
	if err!=nil { return }
	
	// --------------------------------------------------
	
	target,err := d.DecodeString()
	if err!=nil { return }
	
	targid,err := d.DecodeUint64()
	if err!=nil { return }
	
	// --------------------------------------------------
	
	l := len(s.Data)
	f := len(s.Freespace)
	pos_1 := modulo(hashnum,l)
	pos_2 := pos_1
	
	var hfs bool
	rawdata := true
	switch meta {
	case META_Raw: break
	case META_OuterRedirect: hfs,rawdata = true,false // Just drop the redirect message.
	default:
		// Illegal message type, abort.
		s.i_PutResponse(w,target,targid,RESP_Illegal)
		return
	}
	if !hfs {
		var fs Freespace
		if pos_2<f { fs = s.Freespace[pos_2] }
		hfs = s.Data[pos_2]!=nil
		if fs!=nil && hfs { hfs = fs.HasFreeSpace(len(key),len(value)) }
	}
	if !hfs {
		for i := 0 ; i<l; i++ {
			var fs Freespace
			if i<f { fs = s.Freespace[i] }
			hfs = s.Data[i]!=nil
			if fs!=nil && hfs { hfs = fs.HasFreeSpace(len(key),len(value)) }
			if hfs {
				pos_2 = i
				break
			}
		}
	}
	if !hfs {
		s.i_PutResponse(w,target,targid,RESP_IoError,"No Disk Space")
		return
	}
	var wg sync.WaitGroup
	var reterr error
	CB := errsetter(&wg,&reterr)
	
	store := s.Data[pos_2]
	if store==nil { store = s.Redirects }
	if pos_2<f && store!=s.Redirects && !rawdata {
		f := s.Freespace[pos_2]
		if f!=nil {
			if !f.HasFreeSpace(len(key),len(value)) { store = s.Redirects }
		}
	}
	db_put(store,&badger.Entry{Key:key,Value:value,UserMeta:meta,ExpiresAt:expiresAt},CB)
	
	/*
	If we have put our data into a bucket != the specified target, we need to put a hint into
	the destination or redirect bucket, that points to the choosen destination.
	*/
	if pos_1!=pos_2 && store!=s.Redirects {
		data,_ := msgpack.Marshal(pos_2)
		
		store = s.Data[pos_1]
		if store==nil { store = s.Redirects }
		db_put(store,&badger.Entry{Key:key,Value:data,UserMeta:META_InnerRedirect,ExpiresAt:expiresAt},CB)
	}
	
	wg.Wait()
	
	if reterr!=nil {
		s.i_PutResponse(w,target,targid,RESP_IoError,reterr.Error())
	} else {
		s.i_PutResponse(w,target,targid,RESP_OK)
	}
	
	if pos_1<f { freespacetouch(s.Freespace[pos_1]) }
	if pos_1!=pos_2 && pos_2<f { freespacetouch(s.Freespace[pos_2]) }
}
func (s *Store) Put(w *mlst.WrapNode, d *mlst.MessageReader, msg bufferex.Binary) bool {
	mlst.RetainBinary(d)
	go s.i_Put(w,d,msg)
	return false
}

func (s *Store) Attach(wn *mlst.WrapNode) {
	wn.Handlers[MH_Get] = s.Get
	wn.Handlers[MH_Put] = s.Put
}


// ##

