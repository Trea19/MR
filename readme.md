# MR
6.5840 Lab 1,  Spring 2023

PASSED ALL TESTS

![image](/img/Screenshot.png)

## MR - sequential

```shell
$ cd main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```

## MR - distributed

```shell
$ cd main
$ bash test-mr.sh
```

You may see some errors from the Go RPC package that look like `rpc.Register: method "Done" has 1 input parameters; needs exactly three`. Ignore these messages; registering the coordinator as an RPC server checks if all its methods are suitable for RPCs (have 3 inputs); we know that `Done` is not called via RPC.


