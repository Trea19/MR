# MR
6.5840 Lab 1,  Spring 2023

## MR - sequential

```shell
$ cd main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```

