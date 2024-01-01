STM(software transactional memory) implementation in Go.

This implementation is based on the paper "Transactional Locking II" (TL2) proposed by Dice et al.


## Example

The core API is simple enough, just `Load()`, `Store()`, and `Atomically()`.

```
var v Var
stm.Atomically(func(txn *Txn) {
	v.Store(txn, 42)
	x, err := v.Load(txn)
	if err != nil {
		// error not nil means read conflict, it will retry automatically
	}
})
```

`Run()` API is almost the same with `Atomically()`, with object reuse to get better performance.
Readonly transaction use `Run` API makes zero allocation.

```
var clock VersionClock
var txn Txn
stm.Run(&clock, &txn, func(txn *Txn) {
   ...
})
```

## Benchmark

```
go test -test.run XX -test.bench Benchmark --benchmem
goos: linux
goarch: amd64
pkg: github.com/tiancaiamao/stm
cpu: AMD Ryzen 7 PRO 4750G with Radeon Graphics
BenchmarkReadOnly-16            91309795                15.49 ns/op            0 B/op          0 allocs/op
BenchmarkWriteRead-16            7707915               145.1 ns/op             0 B/op          0 allocs/op
```
