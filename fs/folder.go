package fs

import (
	"context"
	"hash/fnv"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"

	"github.com/prologic/bitcaskfs/store"
)

// Set file owners to the current user,
// otherwise in OSX, we will fail to start.
var uid, gid uint32

func init() {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	uid32, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid32, _ := strconv.ParseUint(u.Gid, 10, 32)
	uid = uint32(uid32)
	gid = uint32(gid32)
}

// A tree node in filesystem, it acts as both a directory and file
type Node struct {
	fs.Inode
	store store.Store

	isLeaf bool   // A leaf of the filesystem tree means it's a file
	path   string // File path to get to the current file

	rwMu    sync.RWMutex // Protect file content
	content []byte       // Internal buffer to hold the current file content
}

// NewRoot returns a file node - acting as a root, with inode sets to 1 and leaf sets to false
func NewRoot(store store.Store) *Node {
	return &Node{
		store:  store,
		isLeaf: false,
	}
}

// List keys under a certain prefix from the store, and output the next hierarchy level
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	parent := n.resolve("")
	log.WithField("path", parent).Debug("Node Readdir")

	entrySet := make(map[string]fuse.DirEntry)

	keys, err := n.store.ListKeys(ctx, parent)
	if err != nil {
		log.WithError(err).WithField("path", parent).Errorf("Failed to list keys from store")
		return nil, syscall.EIO
	}

	for _, key := range keys {
		nextLevel, hasMore := n.nextHierarchyLevel(key, parent)
		if _, exist := entrySet[nextLevel]; exist {
			continue
		}
		entrySet[nextLevel] = fuse.DirEntry{
			Mode: n.getMode(!hasMore),
			Name: nextLevel,
			Ino:  n.inodeHash(nextLevel),
		}
	}

	entries := make([]fuse.DirEntry, 0, len(entrySet))
	for _, e := range entrySet {
		entries = append(entries, e)
	}
	return fs.NewListDirStream(entries), fs.OK
}

// Returns next hierarchy level and tells if we have more hierarchies
// path "/foo", parent "/" => "foo"
func (n *Node) nextHierarchyLevel(path, parent string) (string, bool) {
	baseName := strings.TrimPrefix(path, parent)
	hierarchies := strings.SplitN(baseName, string(filepath.Separator), 2)
	return filepath.Clean(hierarchies[0]), len(hierarchies) >= 2
}

// resolve acts as `filepath.Join`, but we want the '/' separator always
func (n *Node) resolve(fileName string) string {
	return n.path + string(filepath.Separator) + fileName
}

// Mkdir implements the NodeMkdirer interface and the `mkdir` operation.
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := n.resolve(name)
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}

	if err := n.store.PutValue(ctx, fullPath, []byte{}); err != nil {
		log.WithError(err).WithField("path", fullPath).Errorf("Failed to write keys to Bitcask")
		return nil, syscall.EIO
	}

	child := Node{
		path:  fullPath,
		store: n.store,
	}

	return n.NewInode(ctx, &child, fs.StableAttr{Mode: child.getMode(child.isLeaf), Ino: n.inodeHash(child.path)}), fs.OK
}

// Lookup finds a file under the current node(directory)
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := n.resolve(name)
	log.WithField("path", fullPath).Debug("Node Lookup")
	keys, err := n.store.ListKeys(ctx, fullPath)
	if err != nil {
		log.WithError(err).WithField("path", fullPath).Errorf("Failed to list keys from store")
		return nil, syscall.EIO
	}
	if len(keys) == 0 {
		return nil, syscall.ENOENT
	}
	key := keys[0]
	child := Node{
		path:  fullPath,
		store: n.store,
	}
	if key == fullPath {
		child.isLeaf = true
	} else if strings.HasPrefix(key, fullPath+string(filepath.Separator)) {
		child.isLeaf = false
	} else {
		return nil, syscall.ENOENT
	}
	return n.NewInode(ctx, &child, fs.StableAttr{Mode: child.getMode(child.isLeaf), Ino: n.inodeHash(child.path)}), fs.OK
}

func (n *Node) getMode(isLeaf bool) uint32 {
	if isLeaf {
		return 0644 | uint32(syscall.S_IFREG)
	} else {
		return 0755 | uint32(syscall.S_IFDIR)
	}
}

// Getattr outputs file attributes
// TODO: how to invalidate them?
func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = n.getMode(n.isLeaf)
	out.Size = uint64(len(n.content))
	out.Ino = n.inodeHash(n.path)
	now := time.Now()
	out.SetTimes(&now, &now, &now)
	out.Uid = uid
	out.Gid = gid
	return fs.OK
}

// Hash file path into inode number, so we can ensure the same file always gets the same inode number
func (n *Node) inodeHash(path string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return h.Sum64()
}

var (
	_ fs.NodeMkdirer   = &Node{}
	_ fs.NodeGetattrer = &Node{}
	_ fs.NodeReaddirer = &Node{}
	_ fs.NodeLookuper  = &Node{}
)
