package main

import (
	"net"

	"github.com/kamijin-fanta/nbd-go"
	"github.com/myml/ks"
	"github.com/myml/ks/storage/file"
)

type KSDeviceFactory struct {
}

func (m *KSDeviceFactory) NewClient(remoteAddr net.Addr) nbd.DeviceConnection {
	fd, err := NewKSDeviceConnection()
	if err != nil {
		panic(err)
	}
	return fd
}

var _ nbd.DeviceConnection = &KSDeviceConnection{}

func NewKSDeviceConnection() (*KSDeviceConnection, error) {
	size := uint64(1024 * 1024 * 1024 * 1024)
	stream := ks.NewStream(ks.WithChunkSize(1024*1024*4), ks.WithStorage(&file.Storage{}))
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
	return "default", "default exports", m.size, 1024 * 4, 0 // 4K Block
}

func (m *KSDeviceConnection) Read(offset uint64, length uint32) ([]byte, nbd.Errornum) {
	buff := make([]byte, length)
	_, err := m.f.ReadAt(buff, int64(offset))
	if err != nil {
		return nil, nbd.EIO
	}
	return buff, 0
}

func (m *KSDeviceConnection) Write(offset uint64, buff []byte) nbd.Errornum {
	_, err := m.f.WriteAt(buff, int64(offset))
	if err != nil {
		return nbd.EIO
	}
	return 0
}

func (m *KSDeviceConnection) Flush() nbd.Errornum {
	return 0
}

func (m *KSDeviceConnection) Close() {
}
