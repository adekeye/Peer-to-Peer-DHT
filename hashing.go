package main

import (
	"crypto/sha1"
	"math/big"
)

func hash(ip string) []byte {
	myHash := sha1.New()
	myHash.Write([]byte(ip))
	return myHash.Sum(nil)
}
func powerOffset(id []byte, exp int, mod int) int64 {
	offsetCpy := make([]byte, len(id)) //// make a copy of existing slice
	copy(offsetCpy, id)

	idInt := big.Int{}
	idInt.SetBytes(id)

	two := big.NewInt(2) //Obtain offset
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)

	sum := big.Int{} //sum
	sum.Add(&idInt, &offset)

	ceil := big.Int{} //Obtain ceiling
	ceil.Exp(two, big.NewInt(int64(mod)), nil)
	idInt.Mod(&sum, &ceil) // Apply the Modulus

	return idInt.Int64()
}

func consistentHashing(id string) int {
	myHash := hash(id)
	hashOffset := powerOffset(myHash, 1, int(chordRing.RingSize))
	return int(hashOffset)
}
