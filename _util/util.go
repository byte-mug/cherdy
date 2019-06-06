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


package util

import (
	"github.com/hashicorp/memberlist"
	uuid "github.com/hashicorp/go-uuid"
	"flag"
	"log"
	"crypto/md5"
)

var name = flag.String("name","","Memberlist: node name")
var defname = flag.Bool("defname",false,"Memberlist: use the default node name")

var local = flag.Bool("local",false,"Memberlist: Optimize for localhost instead of LAN")
var wan = flag.Bool("wan",false,"Memberlist: Optimize for WAN instead of LAN")

var key = flag.String("key","","Memberlist: Encryption key (password)")

var addr = flag.String("addr","","Memberlist: bind addr")
var port = flag.Int("port",0,"Memberlist: bind port")

var advaddr = flag.String("advaddr","","Memberlist: advertise addr")
var advport = flag.Int("advport",0,"Memberlist: advertise port")

func GetConfig() (cfg *memberlist.Config) {
	if *local {
		cfg = memberlist.DefaultLocalConfig()
	} else if *wan {
		cfg = memberlist.DefaultWANConfig()
	} else {
		cfg = memberlist.DefaultLANConfig()
	}
	
	if *defname {
		/* Do nothing. */
	} else if len(*name)!=0 {
		cfg.Name = *name
	} else {
		s,e := uuid.GenerateUUID()
		if e!=nil { log.Fatalf("%v",e) }
		cfg.Name = s
	}
	
	if len(*key)!=0 {
		n := new([md5.Size]byte)
		*n = md5.Sum([]byte(*key))
		kr,e := memberlist.NewKeyring(nil,n[:])
		if e!=nil { log.Fatalf("%v",e) }
		cfg.Keyring = kr
		cfg.SecretKey = n[:]
	}
	
	if len(*addr)!=0 {
		a := *addr
		cfg.BindAddr = a
		cfg.AdvertiseAddr = a
	}
	if *port!=0 {
		p := *port
		cfg.BindPort = p
		cfg.AdvertisePort = p
	}
	
	if len(*advaddr)!=0 {
		a := *advaddr
		cfg.AdvertiseAddr = a
	}
	if *advport!=0 {
		p := *advport
		cfg.AdvertisePort = p
	}
	
	return cfg
}

