package stm

import (
	"errors"
	"sync/atomic"
)

type Txn struct {
	tmp [5]*Var // try to avoid allocation as much as possible

	rv       uint64 // read version
	readSet  []*Var
	writeSet map[*Var]interface{}

	retry  bool
	locked []*Var // Need to free lock before retry
}

type Var struct {
	lock versionedWriteLock
	val  interface{}
}

// 1 bit for lock and 63 bits for version.
type versionedWriteLock uint64

func (l *versionedWriteLock) load() (locked bool, version uint64) {
	v := atomic.LoadUint64((*uint64)(l))
	locked = ((v >> 63) > 0)
	version = v & ((1 << 63) - 1)
	return
}

func (l *versionedWriteLock) tryAcquire() bool {
	v := atomic.LoadUint64((*uint64)(l))
	locked := ((v >> 63) > 0)
	if locked { // locked already
		return false
	}
	v1 := v | (1 << 63)
	return atomic.CompareAndSwapUint64((*uint64)(l), v, v1)
}

func (l *versionedWriteLock) commit(v uint64) {
	locked, _ := l.load()
	if !locked {
		panic("commit() something is wrong")
	}
	atomic.StoreUint64((*uint64)(l), v)
}

func (l *versionedWriteLock) release() {
	locked, version := l.load()
	if !locked {
		panic("release() something is wrong")
	}
	atomic.StoreUint64((*uint64)(l), version)
}

type VersionClock uint64

func (global *VersionClock) load() uint64 {
	return atomic.LoadUint64((*uint64)(global))
}

func (global *VersionClock) increment() uint64 {
	return atomic.AddUint64((*uint64)(global), 1)
}

var global VersionClock

func Atomically(speculative func(*Txn)) {
	var txn Txn
	txn.readSet = txn.tmp[:0]
	runWithTxn(&global, &txn, speculative)
}

func runWithTxn(global *VersionClock, txn *Txn, speculative func(*Txn)) {
	for i := 0; ; i++ {
		txn.retry = false
		// Step1: sample global version-clock
		txn.rv = global.load()

		// Step2: run through a speculative execution
		speculative(txn)
		if txn.retry {
			continue
		}

		// optimize: if this is a read-only txn, all works done.
		if len(txn.writeSet) == 0 {
			return
		}

		// Step3: lock the write-set
		if txn.locked == nil {
			txn.locked = make([]*Var, 0, len(txn.writeSet))
		}
		for writeVar := range txn.writeSet {
			if ok := writeVar.lock.tryAcquire(); !ok {
				abortAndRetry(txn)
				break
			}
			txn.locked = append(txn.locked, writeVar)
		}
		if txn.retry {
			continue
		}

		// Step4: increment global version-clock
		writeVersion := global.increment()

		// Step5: validate the read-set
		if writeVersion == txn.rv+1 {
			// optimize: it means we are the only writer, so no need to validate the read set
		} else {
			for _, readVar := range txn.readSet {
				locked, version := readVar.lock.load()
				var lockedByMe bool
				if locked {
					_, lockedByMe = txn.writeSet[readVar]
				}
				if (locked && !lockedByMe) || version > txn.rv {
					abortAndRetry(txn)
					break
				}
			}
			if txn.retry {
				continue
			}
		}

		// Step6: commit and free lock
		commitTxn(txn, writeVersion)
		return
	}
}

// Run differs from Atomically that it use separate VersionClock instead of a global one,
// and it reuse Txn object to get better performance.
func Run(global *VersionClock, txn *Txn, speculative func(*Txn)) {
	resetForReuse(txn)
	runWithTxn(global, txn, speculative)
}

var errRetry = errors.New("transaction conflicts, should retry")

func (v *Var) Load(txn *Txn) (interface{}, error) {
	// The transactional load first checks (using a Bloom filter [24]) to see if the
	// load address already appears in the write-set. If so, the transactional load
	// returns the last value written to the address.
	if v, ok := txn.writeSet[v]; ok {
		return v, nil
	}

	// A load instruction sampling the associated lock is inserted.
	locked, version1 := v.lock.load()
	if locked || version1 > txn.rv {
		abortAndRetry(txn)
		return nil, errRetry
	}

	// The original load.
	val := v.val

	// post-validation code checking that the location’s versioned write-lock is free and has not changed
	locked, version2 := v.lock.load()
	if version1 != version2 || version2 > txn.rv || locked {
		abortAndRetry(txn)
		return nil, errRetry
	}

	// Don't forget this step!
	txn.readSet = append(txn.readSet, v)
	return val, nil
}

func (v *Var) Store(txn *Txn, val interface{}) {
	if txn.writeSet == nil {
		txn.writeSet = make(map[*Var]interface{}, 5) // lazy initialize to get better performance
	}
	txn.writeSet[v] = val
}

func abortAndRetry(txn *Txn) {
	txn.rv = 0
	txn.readSet = txn.readSet[:0]
	if len(txn.locked) > 0 {
		// Don't forget to release the locks!
		for _, writeVar := range txn.locked {
			writeVar.lock.release()
		}
		txn.locked = txn.locked[:0]
	}
	clear(txn.writeSet)
	txn.retry = true
}

func resetForReuse(txn *Txn) {
	txn.readSet = txn.readSet[:0]
	txn.locked = txn.locked[:0]
	clear(txn.writeSet)
}

func commitTxn(txn *Txn, wv uint64) {
	for writeVar, val := range txn.writeSet {
		writeVar.val = val
		writeVar.lock.commit(wv)
	}
}
