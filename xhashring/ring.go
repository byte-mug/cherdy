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
	"github.com/byte-mug/golibs/chordhash"
	avl "github.com/emirpasic/gods/trees/avltree"
	"github.com/byte-mug/golibs/concurrent/sortlist"
	"github.com/emirpasic/gods/utils"
	"sync"
	"crypto/md5"
	"time"
	
	"fmt"
)

func md5hf(s string) string {
	h := md5.Sum([]byte(s))
	return string(h[:])
}

type Entry struct{
	Name, Hash string
	
	lock sync.RWMutex
	
	alive bool
	tmout time.Time
}
func (e *Entry) String() string { return fmt.Sprintf("{%q  #: %x}",e.Name,e.Hash) }
func (e *Entry) join() {
	e.lock.Lock()
	defer e.lock.Unlock()
	
	e.alive = true
	e.tmout = time.Time{}
}
func (e *Entry) leave(death time.Duration) {
	e.lock.Lock()
	defer e.lock.Unlock()
	
	e.tmout = time.Now().Add(death)
	e.alive = false
}
func (e *Entry) testDeath(cur time.Time) bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	if e.alive { return false }
	return cur.After(e.tmout)
}

type Table struct{
	HashFunc func(string)string
	
	NodeDeath time.Duration
	
	index sortlist.Sortlist
	ring  chordhash.Circle
	
	lck sync.Mutex
	wg  sync.WaitGroup
	
	clck sync.Mutex
}
func (t *Table) lock() {
	t.lck.Lock()
	t.wg.Add(1)
}
func (t *Table) unlock() {
	t.wg.Done()
	t.lck.Unlock()
}
func (t *Table) Debug_Dump() {
	t.lck.Lock()
	defer t.lck.Unlock()
	
	fmt.Println("{")
	for n := t.ring.Tree.Left(); n!=nil; n = n.Next() {
		//fmt.Printf("\t%x -> %q\n",n.Key,n.Value.(*Entry).Name)
		fmt.Println("\t",n.Value)
	}
	fmt.Println("}")
}

func (t *Table) dump() {
	fmt.Println("{")
	for n := t.ring.Tree.Left(); n!=nil; n = n.Next() {
		//fmt.Printf("\t%x -> %q\n",n.Key,n.Value.(*Entry).Name)
		fmt.Println("\t",n.Value)
	}
	fmt.Println("}")
}

func (t *Table) Init() {
	if t.HashFunc==nil { t.HashFunc = md5hf }
	if t.NodeDeath==0 { t.NodeDeath = time.Hour*24*2 }
	
	t.index.Cmp = utils.StringComparator
	t.ring.Init()
}

func (t *Table) Step(to *avl.Node) *avl.Node {
	t.wg.Wait()
	return t.ring.Step(to)
}
func (t *Table) StepReverse(to *avl.Node) *avl.Node {
	t.wg.Wait()
	return t.ring.StepReverse(to)
}


func (t *Table) Next(id string) (node *avl.Node) {
	t.wg.Wait()
	return t.ring.Next(id)
}
func (t *Table) NextOrEqual(id string) (node *avl.Node) {
	t.wg.Wait()
	return t.ring.NextOrEqual(id)
}
func (t *Table) Prev(id string) (node *avl.Node) {
	t.wg.Wait()
	return t.ring.Prev(id)
}
func (t *Table) PrevOrEqual(id string) (node *avl.Node) {
	t.wg.Wait()
	return t.ring.PrevOrEqual(id)
}
func (t *Table) Left() *avl.Node {
	t.wg.Wait()
	return t.ring.Tree.Left()
}

/*
A node, with the hashring extension enabled joins the cluster.
*/
func (t *Table) Join(name string) {
	v := t.index.Lookup(name)
	if v!=nil {
		v.Value.(*Entry).join()
		return
	}
	
	ent := &Entry{Name:name,Hash:t.HashFunc(name)}
	
	t.lock()
	defer t.unlock()
	
	v = t.index.Lookup(name)
	if v!=nil {
		v.Value.(*Entry).join()
		return
	}
	
	ent.join()
	t.index.Insert(name,ent)
	t.ring.Tree.Put(ent.Hash,ent)
}

/*
A node, with the hashring extension enabled leaves the cluster (Death).
*/
func (t *Table) Leave(name string) {
	v := t.index.Lookup(name)
	if v!=nil {
		v.Value.(*Entry).leave(t.NodeDeath)
	}
}

/*
A node, with the hashring extension enabled turns off the hashring extension.

On nodes, with the hashring extension already turned off, this has no effect.
*/
func (t *Table) Invalidate(name string) {
	v := t.index.Lookup(name)
	if v!=nil {
		e := v.Value.(*Entry)
		t.lock()
		defer t.unlock()
		t.index.Delete(e.Name)
		t.ring.Tree.Remove(e.Hash)
	}
}

/*
A node turns on the hashring extension.

On nodes, with the hashring extension already turned on, this has no effect.
*/
func (t *Table) Validate(name string) {
	v := t.index.Lookup(name)
	if v!=nil {
		/* Already turned on. */
		return
	}
	
	ent := &Entry{Name:name,Hash:t.HashFunc(name)}
	
	t.lock()
	defer t.unlock()
	
	v = t.index.Lookup(name)
	if v!=nil {
		v.Value.(*Entry).join()
		return
	}
	
	ent.join()
	t.index.Insert(name,ent)
	t.ring.Tree.Put(ent.Hash,ent)
}


func (t *Table) death() (es []*Entry) {
	now := time.Now()
	t.clck.Lock()
	defer t.clck.Unlock()
	l := t.Left()
	for l!=nil {
		e := l.Value.(*Entry)
		if e.testDeath(now) { es = append(es,e) }
		l = t.Step(l)
	}
	return
}
func (t *Table) Cleanup() {
	es := t.death()
	t.lock()
	defer t.unlock()
	for _,e := range es {
		t.index.Delete(e.Name)
		t.ring.Tree.Remove(e.Hash)
	}
}


