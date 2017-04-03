package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v5"
)

type GCOptions struct {
	RedisMasterFile string
	GcThreshold     int
	GcDatabases     string
}
type GCState struct {
	opts          GCOptions
	currentMaster string
	currentDB     int
	redis         *redis.Client // current connection
	keySuffixes   []string
}

func (s *GCState) key(msgId, suffix string) string {
	return fmt.Sprintf("%s:%s", msgId, suffix)
}

func (s *GCState) keys(msgId string) []string {
	res := make([]string, 0)
	for _, suffix := range s.keySuffixes {
		res = append(res, s.key(msgId, suffix))
	}
	return res
}

func (s *GCState) msgId(key string) string {
	re := regexp.MustCompile("^(msgid:[^:]+:[-0-9a-f]+):.*$")
	matches := re.FindStringSubmatch(key)
	if len(matches) == 0 {
		panic(fmt.Errorf("msgid could not be extracted from key '%s'", key))
	}
	return matches[1]
}

func (s *GCState) gcKey(key string, threshold uint64) (bool, error) {
	v, err := s.redis.Get(key).Result()
	if err != nil {
		return false, err
	}
	expires, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return false, err
	}
	t := time.Duration(expires-threshold) * time.Second
	if expires >= threshold {
		logDebug("key %s expires in %s", key, t)
		return false, err
	}
	logDebug("key %s has expired %s ago", key, t)
	msgId := s.msgId(key)
	keys := s.keys(msgId)
	// logDebug("deleting keys: %s", strings.Join(keys, ", "))
	_, err = s.redis.Del(keys...).Result()
	return err == nil, err
}

func (s *GCState) garbageCollectKeys(db int) {
	var total, expired int
	var cursor uint64
	defer logInfo("expired %d keys out of %d in db %d", expired, total, db)
	ticker := time.NewTicker(1 * time.Second)
collecting:
	for _ = range ticker.C {
		if s.getMaster(db) {
			if cursor == 0 {
				logInfo("starting SCAN on db %d", db)
			}
			var err error
			var keys []string
			keys, cursor, err = s.redis.Scan(cursor, "msgid:*:expires", 1000).Result()
			if err != nil {
				logError("starting over: %v", err)
				cursor = 0
				total = 0
				expired = 0
				continue collecting
			}
			logDebug("retrieved %d keys from db %d", len(keys), db)
			total += len(keys)
			threshold := time.Now().Unix() + int64(s.opts.GcThreshold)
			for _, key := range keys {
				collected, err := s.gcKey(key, uint64(threshold))
				if err != nil {
					logError("starting over: %v", err)
					cursor = 0
					total = 0
					expired = 0
					goto collecting
				}
				if collected {
					expired += 1
				}
			}
			if cursor == 0 || interrupted {
				return
			}
		}
	}
}

func (s *GCState) getMaster(db int) bool {
	server := ReadRedisMasterFile(opts.RedisMasterFile)
	if s.currentMaster != server || s.currentDB != db {
		s.currentMaster = server
		s.currentDB = db
		if server == "" {
			if s.redis != nil {
				s.redis.Close()
			}
			s.redis = nil
		} else {
			s.redis = redis.NewClient(&redis.Options{Addr: server, DB: db})
		}
	}
	return s.redis != nil
}

func RunGarbageCollectKeys(opts GCOptions) error {
	state := &GCState{opts: opts}
	state.keySuffixes = []string{"status", "ack_count", "timeout", "delay", "attempts", "exceptions", "mutex", "expires"}
	for _, s := range strings.Split(opts.GcDatabases, ",") {
		if s != "" {
			db, err := strconv.Atoi(s)
			if err != nil {
				logError("%v", err)
				continue
			}
			state.garbageCollectKeys(db)
		}
	}
	if state.redis != nil {
		return state.redis.Close()
	}
	return nil
}
