package main

import (
	"encoding/json"
	"math"
)

func testMain() {
	testLog2()
}

func testJSON() {
	str := "{\"do\":\"stabilize-ring\"}"
	s := &action{}
	s.unmarshal(str)
	println(s.Do)

	a := &action{Do: fixRing, Mode: immediate}
	println(a.marshal())

	a = &action{Mode: immediate}
	println(a.marshal())

	d := &data{Key: "wow"}
	println(d.marshal())

	hq := &hashQuery{Data: d, Do: get, RespondTo: "Me-123"}
	println(hq.marshal())

	d2 := &data{Key: "wow2"}

	arr := [1]data{*d2}

	bytarr, err := json.Marshal(&arr)
	if validate(err) {
		println(string(bytarr))
	}

	f := &fileInfo{filename: "datafile"}
	fb := f.read()
	c := &config{}
	c.unmarshal(string(fb))
	l := c.LiveChanges
	println(l[0].NodeID)
	println(l[1].Time)
	println(l[2].Action)
}

func testFingerTable() {
	filename := "datafile"
	fileInfo := &fileInfo{filename: filename}
	fbyte := fileInfo.read()
	chordRing = &config{}
	chordRing.unmarshal(string(fbyte))

	f := &fingerTable{}
	f.init()
	f.add(1, "a")
	f.add(0, "b")
	f.add(4, "c")
	f.add(7, "d")
	f.delete(1)
}

func modfunc() {
	s := "127.0.0.1"
	b := hash(s)
	i := powerOffset(b, 1, 5)
	logger.Info(i)

	s = "127.0.0.2"
	b = hash(s)
	i = powerOffset(b, 1, 5)
	logger.Info(i)

	s = "127.0.0.3"
	b = hash(s)
	i = powerOffset(b, 1, 5)
	logger.Info(i)

}

func testDictionary() {
	gd := myDict{}
	gd.add("15", &node{})
	gd.add("24", &node{})

	logger.Info(gd.getSuccessor("4"))
	logger.Info(gd.getSuccessor("19"))
	logger.Info(gd.getSuccessor("16"))
	logger.Info(gd.getSuccessor("27"))

	logger.Info(gd.getPredecessor("4"))
	logger.Info(gd.getPredecessor("19"))
	logger.Info(gd.getPredecessor("16"))
	logger.Info(gd.getPredecessor("27"))

}

func testLog2() {
	diff := 15 - 5
	index := int(math.Log2(float64(diff)))
	println(index)

	diff = 5 - 15
	index = int(math.Log2(float64(diff)))
	println(index)
}
