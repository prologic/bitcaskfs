package fs

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	"github.com/prologic/bitcaskfs/config"
	"github.com/prologic/bitcaskfs/store"
)

type Server struct {
	*fuse.Server
	mountPoint string
}

// 200ms is enough for an operation to complete
var cacheDuration = 200 * time.Millisecond

func MustMount(mountPoint string, store store.Store) *Server {
	opts := &fs.Options{
		AttrTimeout:  &cacheDuration,
		EntryTimeout: &cacheDuration,
		MountOptions: fuse.MountOptions{
			Options: config.MountOptions,
			Debug:   false,
			FsName:  "bitcaskfs",
		},
	}
	server, err := fs.Mount(mountPoint, NewRoot(store), opts)
	if err != nil {
		log.WithError(err).Fatal("Failed to mount")
		return nil
	}
	return &Server{
		Server:     server,
		mountPoint: mountPoint,
	}
}

func (s *Server) ListenForUnmount() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	sig := <-c
	log.Infof("Got %s signal, unmounting %q...", sig, s.mountPoint)
	err := s.Unmount()
	if err != nil {
		log.WithError(err).Errorf("Failed to unmount, try %q manually.", "umount "+s.mountPoint)
	}
	<-c // Double ctrl+c
	log.Warn("Force exiting...")
	os.Exit(1)
}
