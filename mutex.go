package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v7"
)

const (
	prefix              = `lock:`
	defaultLockDuration = time.Millisecond * 300
	respExists          = `EXISTS`
	respOK              = `OK`

	lockCommand = `
if redis.call("EXISTS", KEYS[1]) ~= 0
then
    return "` + respExists + `"
else
    return redis.call("SET", KEYS[1], 1, "NX", "PXAT", ARGV[1]) -- return OK
end`
	deleteCommand = `
if redis.call("EXISTS", KEYS[1]) ~= 0
then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`
	tickerDuration = time.Millisecond * 50
)

var lockScript = redis.NewScript(lockCommand)
var deleteScript = redis.NewScript(deleteCommand)

type Locker struct {
	client       *redis.ClusterClient
	key          string
	lockDuration time.Duration
}

func NewMutex(client *redis.ClusterClient, key string, lockDuration time.Duration) *Locker {
	if lockDuration < defaultLockDuration {
		lockDuration = defaultLockDuration
	}
	return &Locker{
		client:       client,
		key:          prefix + key,
		lockDuration: lockDuration,
	}
}

func (l *Locker) Lock() {
	waiting := make(chan struct{})
	defer close(waiting)

	ctx, ctxClose := context.WithTimeout(context.Background(), l.lockDuration)
	defer ctxClose()

	retry := time.NewTicker(tickerDuration)
	defer retry.Stop()

	go func() {
		defer func() {
			waiting <- struct{}{}
		}()
		//	опрашивает в первый раз, без тикера
		if l.TryLock() {
			return
		}
		// ждем от моря погоды
		for {
			select {
			case <-retry.C:
				if l.TryLock() {
					return
				}
			case <-ctx.Done():
				return
			}
		}

	}()

	<-waiting
}

func (l *Locker) Unlock() {
	//helper.Stamper("Start Unlock")
	//defer helper.Stamper("End Unlock")

	waiting := make(chan struct{})
	defer close(waiting)

	ctx, ctxClose := context.WithTimeout(context.Background(), l.lockDuration)
	defer ctxClose()

	retry := time.NewTicker(tickerDuration)
	defer retry.Stop()

	go func() {
		defer func() {
			waiting <- struct{}{}
		}()

		if l.TryUnlock() {
			return
		}
		for {
			select {
			case <-retry.C:
				if l.TryUnlock() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	<-waiting
}

func (l *Locker) TryLock() bool {
	resp, hasError := lockScript.Run(l.client, []string{l.key}, time.Now().Add(l.lockDuration).UnixMilli()).Result()
	return hasError == nil && resp.(string) == respOK
}

func (l *Locker) TryUnlock() bool {
	_, hasError := deleteScript.Run(l.client, []string{l.key}).Result()
	return hasError == nil
}
