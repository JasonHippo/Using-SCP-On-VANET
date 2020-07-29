package main

// Usage:
//   lunch [-seed N] CONFIGFILE

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"

	"time"

	"github.com/BurntSushi/toml"
	"github.com/JasonHippo/scp"
)

type valType string //定義新的type valType 為string

func (v valType) Less(other scp.Value) bool {
	return v < other.(valType)
}

func (v valType) Combine(other scp.Value, slotID scp.SlotID) scp.Value {
	if slotID%2 == 0 {
		if v > other.(valType) {
			return v
		}
	} else if v < other.(valType) {
		return v
	}
	return other
}

func (v valType) IsNil() bool {
	return v == ""
}

func (v valType) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, v)
	return buf.Bytes()
}

func (v valType) String() string {
	return string(v)
}

type nodeconf struct { //定義新的type nodeconf的架構
	Q  scp.QSet //Q為QSet類型
	FP int
	FQ int
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) //讓output message 的timestamp 印到microseconds
	seed := flag.Int64("seed", 1, "RNG seed ")
	//flag.Int64:Int64 defines an int64 flag with specified name, default value, and usage string.
	//所以seed是名稱，1是預設值，後面是提示訊息
	delay := flag.Int("delay", 100, "random delay limit in milliseconds") //
	//delay 被使用在simulate network delay, 作為call func Delay的參數傳進去
	//如果這個delay降低，甚至放到10ms，速度會快很多
	goodNodes := flag.Int("goodNodes", 0, "set up the number of goodNodes")
	badNodes := flag.Int("badNodes", 0, "set up the number of badNodes")
	// 這邊是設定好的節點以及壞的節點，在command line中做設定，不用每次到程式之中做更動
	flag.Parse()

	// log.Fatalf("%d %d \n", *goodNodes, *badNodes)
	
	if *goodNodes == *badNodes && *goodNodes == 0 {
		log.Fatal("Please set up your goodNodes and BadNodes")
	}// 除錯，若沒設定節點數量，程式在此中止
	
	rand.Seed(*seed)
	//Seed uses the provided seed value to initialize the default Source to a deterministic state

	if flag.NArg() < 1 { // NArg is the number of arguments remaining after flags have been processed.
		log.Fatal("usage: lunch [-seed N] CONFFILE")
		//Fatal is equivalent to Print() followed by a call to os.Exit(1).
	}
	confFile := flag.Arg(0)
	//flag.Arg:Arg returns the i'th command-line argument.
	//這個confFile是我設定的toml檔，後面則是對這個檔案做decode跟處理,印出來是檔名跟路徑
	confBits, err := ioutil.ReadFile(confFile)
	//ioutil.ReadFile:ReadFile reads the file named by filename and returns the contents.
	if err != nil {
		log.Fatal(err)
	}
	var conf map[string]nodeconf
	_, err = toml.Decode(string(confBits), &conf)
	if err != nil {
		log.Fatal(err)
	}

	nodesCounter := 0; // 計算toml中有多少節點
	nodes := make(map[scp.NodeID]*scp.Node)
	ch := make(chan *scp.Msg)
	for nodeID, nconf := range conf {
		nodesCounter++
		node := scp.NewNode(scp.NodeID(nodeID), nconf.Q, ch, nil)
		node.FP, node.FQ = nconf.FP, nconf.FQ
		nodes[node.ID] = node
		//nconf是節點可以選的slices清單跟選幾個的設定檔
		go node.Run(context.Background())  
	}
		
		// TODO 在node.go的run func，尚未看懂
										   // TODO 不懂為什麼 Run.handle.handle裡的msg在這裡判斷topic
	// }
	
	if nodesCounter != *goodNodes + *badNodes {
		log.Fatalf("Total nodes from your toml is %d\n but your goodNodes + badNodes is %d\n", nodesCounter, *goodNodes + *badNodes)
	} // 除錯，若設定的好壞節點與toml裡總節點數不同，程式在此中止
	
	for slotID := scp.SlotID(1); slotID < 2; slotID++ { //設定跑幾個slot
		beginTime := time.Now()
		msgs := make(map[scp.NodeID]*scp.Msg) // holds the latest message seen from each node

		/*		for _, node := range nodes { //這段是原始的，隨機nominate
				msgs[node.ID] = nil
				// for _,  是foreach的用法
				// New slot! Nominate something.
				//rand.Intn:Intn returns, as an int, a non-negative pseudo-random number in [0,n) from the default Source.
				//Intn 從預設的來源 回復一個非負數 非假的隨機 從(0,n)的整數

				nomMsg := scp.NewMsg(node.ID, slotID, node.Q, &scp.NomTopic{X: scp.ValueSet{val}}) //隨機選topic
				//node印出來的是 scp.Node=&{Car-Elaine {2 [N:Car-Peja N:Car-Kobe N:Car-Federer]} 0......
				//nomMag,出來的是整串送的訊息 EXTRA *scp.Msg=(C=2 V=Car-Kobe I=1: NOM X=[trafficjam], Y=[]))
				node.Handle(nomMsg)
			}*/
		
		// var defWellNode = 0 // 正常節點選擇的topic 
		// var goodnodes = 33  // 用來會提名正確值的節點數量 
		// var badnodes = 30  // 設定 
		val := roadcondition[0]

		// log.Fatal(goodnodes)
		for _, node := range nodes { // 2019/12/05 新加設定某些well節點選擇固定的topic
			msgs[node.ID] = nil
			// New slot! Nominate something.
			randomeval := rand.Intn(len(roadcondition)) // 給其他節點隨機選擇用的參數
			if randomeval == 0 {
				randomeval = randomeval + 1 //不讓他選smooth
			}

			// 學長原本的code
			// switch {
			// /*case node.ID == "good-bob" || node.ID == "good-dave" || node.ID == "good-carol": //這裡是我hard code指定哪些節點提名一定會正確
			// 	val = roadcondition[defWellNode]

			// case node.ID == "Bad-M-larry" || node.ID == "lumi" || node.ID == "Bad-M-mark" || node.ID == "alice" || node.ID == "carl" || node.ID == "M-fred":
			// 	val = roadcondition[randomeval]*/

			// case goodnodes > 0:
			// 	val = roadcondition[defWellNode]
			// 	goodnodes = goodnodes - 1

			// case badnodes > 0:
			// 	val = roadcondition[randomeval]
			// 	badnodes = badnodes - 1
			// }

			// 利用隨機的方式選擇節點，比較不hard code
			if *goodNodes == 0 {
				// bad
				(*badNodes)--
				val = roadcondition[randomeval] 
			} else if *badNodes == 0 {
				// good
				(*goodNodes)--
				val = roadcondition[0]
			}else if choose := rand.Intn(1); choose == 0 {
				// good
				(*goodNodes)--
				val = roadcondition[0]
			} else {
				// bad
				(*badNodes)--
				val = roadcondition[randomeval]
			}

			// fmt.Printf("goodnodes: %d, badnodes: %d \n ", goodnodes, badnodes)
			// fmt.Println(node.ID, slotID, node.Q, &scp.NomTopic{X: scp.ValueSet{val}})
			nomMsg := scp.NewMsg(node.ID, slotID, node.Q, &scp.NomTopic{X: scp.ValueSet{val}})
			log.Printf("1.*** node %s 想要提名 topic =%s ", node.ID, val) // print節點挑到的要提名的topic
			node.Handle(nomMsg)// TODO 尚未看懂，還有要了解為何沒有將nomMsg assign 給 msgs[node.ID]
							   // TODO 猜測應該在此完成提名以及投票
							   // TODO 要釐清怎麼使用ch
		}
  
		for msg := range ch {
			// TODO 感覺可以在每個slot之前先清空舊的msg，不太確定
			if msg.I < slotID {
				// discard messages about old slots
				continue
			}

			msgs[msg.V] = msg
			allExt := true
			for _, m := range msgs {
				if m == nil {
					allExt = false
					break
				}
				if _, ok := m.T.(*scp.ExtTopic); !ok {
					allExt = false
					break
				}
			}

			if allExt {
				log.Printf("all externalized, result = %s", msg)
				fmt.Println(time.Since(beginTime))
				log.Printf("all externalized, result=%s", scp.ExtResult)
				for i := 0; i < 35; i++ { //在一round的consensus跑完後，印出節點的quorum
					log.Printf("*** node=%s Quorum=%s ", scp.NodeQuorumarray[i].NodeName, scp.NodeQuorumarray[i].NodeQuorum)
				}

				break
			}

			// TODO 這裡應該是未達成共識 重新投票
			for otherNodeID, otherNode := range nodes {
				if otherNodeID == msg.V {
					continue
				}
				if *delay > 0 {
					otherNode.Delay(rand.Intn(*delay))
				}
				otherNode.Handle(msg) // here
			}
		}
	}
}

var roadcondition = []valType{
	"smooth",              // 好的節點我設定會提名smooth
	"accident, need help", // 其他節點隨機從下面四個選
	"Fogged, be careful",
	"traffcjam",
	"Road Construction",
}
