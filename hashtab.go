package main

type hashtab []string

func (d hashtab) get(key int) string {
	if key < len(d) {
		return d[key]
	}
	panic("Key cannot be found in range")
}

func (d hashtab) put(key int, value string) bool {
	if key < len(d) {
		d[key] = value
		return true
	}
	return false
}

func (d hashtab) remove(key int) bool {
	if key < len(d) {
		var empty string
		d[key] = empty
		return true
	}
	return false
}
