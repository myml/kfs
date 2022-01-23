package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/kamijin-fanta/nbd-go"
	ks "github.com/myml/kfs-ks"
	blob "github.com/myml/kfs-storage-blob"
	gz "github.com/myml/kfs-storage-gz"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	network, addr := "tcp", ":8888"
	url := "s3://test?" +
		"endpoint=127.0.0.1:9000&region=us-west-1&" +
		"disableSSL=true&" +
		"s3ForcePathStyle=true"

	flag.StringVar(&network, "network", network, "listen network")
	flag.StringVar(&addr, "addr", addr, "listen address")
	flag.StringVar(&url, "url", url, "blob url")
	flag.Parse()

	factory := &KSDeviceFactory{
		blobuURL: url,
	}

	fmt.Printf("listen on %s %s\n", network, addr)
	lis, err := net.Listen(network, addr)
	if err != nil {
		panic(err)
	}

	err = nbd.ListenAndServe(lis, factory)
	if err != nil {
		panic(err)
	}
}

type KSDeviceFactory struct {
	blobuURL string
}

func (m *KSDeviceFactory) NewClient(remoteAddr net.Addr) nbd.DeviceConnection {
	fd, err := NewKSDeviceConnection(m.blobuURL)
	if err != nil {
		log.Fatal(err)
	}
	return fd
}

var _ nbd.DeviceConnection = &KSDeviceConnection{}

func NewKSDeviceConnection(url string) (*KSDeviceConnection, error) {
	size := uint64(1024 * 1024 * 1024 * 1024)
	b, err := blob.NewStorage(url)
	if err != nil {
		return nil, fmt.Errorf("new blob: %w", err)
	}
	g := &gz.Storage{RawStorage: b}
	stream := ks.NewStream(ks.WithStorage(g), ks.WithChunkSize(1024*1024*4), ks.WithDebug(true))
	return &KSDeviceConnection{f: stream, size: size}, nil
}

type KSDeviceConnection struct {
	f    *ks.Stream
	size uint64
}

func (m *KSDeviceConnection) ExportList() ([]string, nbd.Errno) {
	panic("implement me")
}

func (m *KSDeviceConnection) Info(export string) (name, description string, totalSize uint64, blockSize uint32, errno nbd.Errno) {
	return "default", "default exports", m.size, 1024 * 1024 * 4, 0 // 4K Block
}

func (m *KSDeviceConnection) Read(offset uint64, length uint32) ([]byte, nbd.Errornum) {
	buff := make([]byte, length)
	_, err := m.f.ReadAt(buff, int64(offset))
	if err != nil {
		log.Println(err)
		return nil, nbd.EIO
	}
	return buff, 0
}

func (m *KSDeviceConnection) Write(offset uint64, buff []byte) nbd.Errornum {
	_, err := m.f.WriteAt(buff, int64(offset))
	if err != nil {
		log.Println(err)
		return nbd.EIO
	}
	return 0
}

func (m *KSDeviceConnection) Flush() nbd.Errornum {
	return 0
}

func (m *KSDeviceConnection) Close() {
}
