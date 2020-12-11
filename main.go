package main

import (
	_ "net/http/pprof"
	"path/filepath"

	"github.com/prologic/bitcask"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/prologic/bitcaskfs/config"
	"github.com/prologic/bitcaskfs/fs"
)

func main() {
	if !config.Execute() {
		return
	}
	db, err := bitcask.Open(config.DBPath)
	if err != nil {
		log.WithError(err).Fatal("error opening database")
	}
	defer db.Close()
	mountPoint, err := filepath.Abs(config.MountPoint)
	if err != nil {
		logrus.WithError(err).WithField("mountPoint", mountPoint).Fatal("Failed to get abs file path")
		return
	}
	server := fs.MustMount(mountPoint, db)
	go server.ListenForUnmount()
	logrus.Infof("Mounted to %q, use ctrl+c to terminate.", mountPoint)
	server.Wait()
}
