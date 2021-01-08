package main

import (
	_ "net/http/pprof"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/prologic/bitcaskfs/config"
	"github.com/prologic/bitcaskfs/fs"
	"github.com/prologic/bitcaskfs/store"
)

func main() {
	if !config.Execute() {
		return
	}

	store, err := store.NewBitcaskStore(config.DBPath)
	if err != nil {
		log.WithError(err).Fatal("error creating store")
		return
	}
	defer store.Close()

	mountPoint, err := filepath.Abs(config.MountPoint)
	if err != nil {
		log.WithError(err).WithField("mountPoint", mountPoint).Fatal("Failed to get abs file path")
		return
	}
	server := fs.MustMount(mountPoint, store)
	go server.ListenForUnmount()
	log.Infof("Mounted to %q, use ctrl+c to terminate.", mountPoint)
	server.Wait()
}
