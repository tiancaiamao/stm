package stm

import (
	"math/rand"
	"sync"
	"testing"
)

func TestSum(t *testing.T) {
	// repeat add1 100000 times concurrently, check the final result is 100000
	var sum Var
	Atomically(func(txn *Txn) {
		sum.Store(txn, 0)
	})

	var wg sync.WaitGroup
	const N = 10
	const M = 100000
	wg.Add(N)
	for x := 0; x < N; x++ {
		go func(sum *Var) {
			for i := 0; i < M; i++ {
				Atomically(func(txn *Txn) {
					v, err := sum.Load(txn)
					if err != nil {
						return
					}
					sum.Store(txn, v.(int)+1)
				})
			}
			wg.Done()
		}(&sum)
	}
	wg.Wait()

	Atomically(func(txn *Txn) {
		total, err := sum.Load(txn)
		if err != nil {
			return
		}
		if total != M*N {
			t.Error("expect 1000000, but get", total)
		}
	})
}

func TestBankTransfer(t *testing.T) {
	var clock VersionClock
	// 10 account
	var account [10]Var

	// initialize, each account balance = 100
	Atomically(func(txn *Txn) {
		for i := 0; i < len(account); i++ {
			account[i].Store(txn, 100)
		}
	})

	// run N bank transfor jobs concurrently
	const N = 24
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(ith int, clock *VersionClock) {
			// repeat M times
			const M = 5000
			for x := 0; x < M; x++ {
				// pick 2 random account.
				from := rand.Intn(10)
				to := rand.Intn(10)
				if from == to {
					continue
				}

				// transfor from one to another
				Atomically(func(txn *Txn) {
					vf, err := account[from].Load(txn)
					if err != nil {
						return
					}
					amount := rand.Intn(vf.(int))
					vt, err := account[to].Load(txn)
					if err != nil {
						return
					}
					if amount > 0 {
						account[from].Store(txn, vf.(int)-amount)
						account[to].Store(txn, vt.(int)+amount)
					}
				})
			}

			wg.Done()
		}(i, &clock)
	}
	wg.Wait()
	Atomically(func(txn *Txn) {
		total := 0
		for _, ac := range account {
			val, err := ac.Load(txn)
			if err != nil {
				return
			}
			total += val.(int)
		}
		if total != 1000 {
			t.Fail()
		}
	})
}

func TestHeap(t *testing.T) {
	// append data to a heap container concurrently, verify it keeps the heap property
	// heap[end]
	var heap [100]Var
	var end Var
	Atomically(func(txn *Txn) {
		end.Store(txn, 0)
	})

	heapAppend := func(gid int, x int, txn *Txn) {
		end1, err := end.Load(txn)
		if err != nil {
			return
		}
		curr := end1.(int)
		parent := curr / 2
		for curr != 0 {
			pv, err := heap[parent].Load(txn)
			if err != nil {
				return
			}
			if pv.(int) <= x {
				break
			}
			heap[curr].Store(txn, pv)
			curr = parent
			parent = parent / 2
		}
		heap[curr].Store(txn, x)
		end.Store(txn, end1.(int)+1)
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(gid int) {
			for j := 0; j < 20; j++ {
				x := rand.Intn(500)
				Atomically(func(txn *Txn) {
					heapAppend(gid, x, txn)
				})
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	Atomically(func(txn *Txn) {
		for i := 0; i < 100; i++ {
			val, _ := heap[i].Load(txn)
			if i*2 < 100 {
				left, _ := heap[i*2].Load(txn)
				if val.(int) > left.(int) {
					t.Fail()
				}
			}
			if i*2+1 < 100 {
				right, _ := heap[i*2+1].Load(txn)
				if val.(int) > right.(int) {
					t.Fail()
				}
			}
		}
	})
}

func TestAPI(t *testing.T) {
	var v Var
	Atomically(func(txn *Txn) {
		v.Load(txn)
		v.Store(txn, 42)
		res, _ := v.Load(txn)
		if res.(int) != 42 {
			t.Fail()
		}
	})
}

func TestWriteSkew(t *testing.T) {
	var a Var
	var b Var

	Atomically(func(txn *Txn) {
		a.Store(txn, 1)
		b.Store(txn, 2)
	})

	var wg sync.WaitGroup
	wg.Add(2)
	ch := make(chan struct{})
	go func() {
		Atomically(func(txn *Txn) {
			<-ch
			va, err := a.Load(txn)
			if err != nil {
				return
			}
			if va.(int) == 1 {
				b.Store(txn, 666)
			}
		})
		wg.Done()
	}()

	go func() {
		Atomically(func(txn *Txn) {
			<-ch
			vb, err := b.Load(txn)
			if err != nil {
				return
			}
			if vb.(int) == 2 {
				a.Store(txn, 42)
			}
		})
		wg.Done()
	}()
	close(ch)
	wg.Wait()

	// The result should be either a=1,b=666  or  a=42,b=2
	// If the final result is a=42,b=666, it means write skew.
	Atomically(func(txn *Txn) {
		va, _ := a.Load(txn)
		vb, _ := b.Load(txn)
		if va.(int) == 42 && vb.(int) == 666 {
			t.Fail()
		}
	})
}

func BenchmarkReadOnly(b *testing.B) {
	var end Var
	var clock VersionClock
	var txn Txn
	Run(&clock, &txn, func(txn *Txn) {
		end.Store(txn, 42)
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Run(&clock, &txn, func(txn *Txn) {
			end.Load(txn)
		})
	}
}

func BenchmarkWriteRead(b *testing.B) {
	var end Var
	var clock VersionClock
	var txn Txn
	Run(&clock, &txn, func(txn *Txn) {
		end.Store(txn, 42)
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Run(&clock, &txn, func(txn *Txn) {
			end.Store(txn, 666)
			end.Load(txn)
		})
	}
}
