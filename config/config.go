package config

import (
	"fmt"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	defaultPprofAddress = "localhost:9327"
)

var (
	MountPoint   string
	MountOptions []string
	Verbose      bool
	EnablePprof  bool

	DBPath string

	// Will be set by go-build
	Version string
	Rev     string
)

var (
	rootCmd = &cobra.Command{
		Use:   fmt.Sprintf("%s [mount-point]", os.Args[0]),
		Short: "Mount Bitcask DB to local file system - find help/update at https://github.com/prologic/bitcaskfs",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmd.Help()
			}
			MountPoint = args[0]
			return nil
		},
	}
)

func init() {
	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: "15:04:05", FullTimestamp: true})

	version := Version
	if version != "" && Rev != "" {
		version = fmt.Sprintf("%s, build %s", version, Rev)
	}
	rootCmd.Version = version

	rootCmd.Flags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
	rootCmd.Flags().BoolVar(&EnablePprof, "enable-pprof", false, fmt.Sprintf("enable runtime profiling data via HTTP server. Address is at %q", "http://"+defaultPprofAddress+"/debug/pprof"))

	rootCmd.Flags().StringSliceVar(&MountOptions, "mount-options", []string{"nonempty"}, "options are passed as -o string to fusermount")
	rootCmd.Flags().StringVarP(&DBPath, "path", "p", "", "path to bitcask database")

	rootCmd.Flags().SortFlags = false
	rootCmd.SilenceErrors = true
}

func Execute() bool {
	if err := rootCmd.Execute(); err != nil {
		logrus.Errorln(err)
		return false
	}
	if len(MountPoint) == 0 {
		return false
	}
	if DBPath == "" {
		return false
	}

	if Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}
	if EnablePprof {
		go func() {
			if err := http.ListenAndServe(defaultPprofAddress, nil); err != nil {
				logrus.WithError(err).Error("Failed to serve pprof")
			}
		}()
	}
	return true
}
