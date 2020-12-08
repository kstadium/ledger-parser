package state

import "fmt"

type KVSet interface {
	Key() string
	Value() string
	Print()
}

type ChannelConfigKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv ChannelConfigKV) Key() string {
	return string(kv.key)
}

func (kv ChannelConfigKV) Value() string {
	return string(kv.value)
}

func (kv ChannelConfigKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type SystemPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPublicKV) Key() string {
	return string(kv.key)
}

func (kv SystemPublicKV) Value() string {
	return string(kv.value)
}

func (kv SystemPublicKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type SystemPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SystemPrivateKV) Key() string {
	return string(kv.key)
}

func (kv SystemPrivateKV) Value() string {
	return string(kv.value)
}

func (kv SystemPrivateKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type UserPublicKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPublicKV) Key() string {
	return string(kv.key)
}

func (kv UserPublicKV) Value() string {
	return string(kv.value)
}

func (kv UserPublicKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type UserPrivateKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv UserPrivateKV) Key() string {
	return string(kv.key)
}

func (kv UserPrivateKV) Value() string {
	return string(kv.value)
}

func (kv UserPrivateKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type FormatVersionKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv FormatVersionKV) Key() string {
	return string(kv.key)
}

func (kv FormatVersionKV) Value() string {
	return string(kv.value)
}

func (kv FormatVersionKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}

type SavePointKV struct {
	key      []byte
	value    []byte
	describe string
}

func (kv SavePointKV) Key() string {
	return string(kv.key)
}

func (kv SavePointKV) Value() string {
	return string(kv.value)
}

func (kv SavePointKV) Print() {
	fmt.Printf("key: %s\nvalue: %s\n\n", kv.key, kv.value)
}
