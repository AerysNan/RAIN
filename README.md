# Realization of RAID-6 based Distributed Storage System
---

* Dependencies:

    - go1.14+
  
    - [golang protobuf](https://github.com/golang/protobuf)

---

* To build the project:
```
  $ make protoc
  $ make bin
 ```
 ---

* To run the projct:
  - manager node:
  ```
  $ cd build
  $ ./manager -p "8080" -s 4 
  ```
  
  >usage: manager [<flags>]
  >
  >Flags:
  >
  >-p, --port="8080"  Listen port of manager server
  >
  >-s, --shard=4      Number of data shards (excl. P and Q)


  - worker node: (if test on single machine, open another terminal/command prompt window) 
  ```
  $ cd build
  $ ./worker -a "127.0.0.1:8080" -p 9091 
  ```

  >usage: worker [<flags>]
  >
  >Flags:
  >
  >-l, --local="127.0.0.1"        Local address
  >
  >-a, --address="127.0.0.1:8080" Address of manager server
  >
  >-p, --port="8081"              Listen port of worker server


  - client node: (if test on single machine, open another terminal/command prompt window) 
  ```
  $ cd build
  $ ./client write --key k --path output -a "127.0.0.1:8080"
  ```
  
  >usage: client [<flags>] <command> [<args> ...]
  >
  >RAIN client
  >
  >Flags:
  >
  > -a, --address="0.0.0.0:8080"  Address of manager server
  >
  >Commands:
  >
  >  write --key=KEY --path=PATH
  >   Write file
  >
  > read --key=KEY --path=PATH
  >   Read file
