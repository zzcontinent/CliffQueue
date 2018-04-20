package main

import (
	CQ "git.woda.ink/woda/services/CliffQueue"
	"time"
	"fmt"
	"runtime"
	log "github.com/xiaomi-tc/log15"
	"strconv"
)

const serviceName = "CliffQueue"

func initLog() {
	logPath := ""
	// 初始化日志
	if runtime.GOOS != "windows" {
		logPath = "/var/log/woda/" + serviceName + "/" + serviceName + ".log"
	} else {
		logPath = "c:/log/woda/" + serviceName + "/" + serviceName + ".log"
	}
	h, _ := log.FileHandler(logPath, log.LogfmtFormat())
	log.Root().SetHandler(h)
}
func main() {
	initLog()
	log.Info("Mem:" + strconv.FormatUint(GetSysMem(), 10))
	pQ := CQ.NewQ()

	//inputStr := []string{"1", "2"}
	//inputStr := []string{"1", "1"}
	//inputStr := []string{"1"}
	//inputStr := []string{"1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2","1", "2"}
	inputStr := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	input := make([]interface{}, 0)
	for _, val := range inputStr {
		input = append(input, val)
	}

	tStart := time.Now() // get current time
	fmt.Println("开始 ", " 时间= ", tStart.String())
	log.Info("开始 " + " 时间= " + time.Now().String() + tStart.String())
	defer func() {
		elapsed := time.Since(tStart)
		fmt.Println("结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())
		log.Info("结束 " + " 时间= " + time.Now().String() + " 耗时:" + elapsed.String())
	}()

	log.Info("Mem:" + strconv.FormatUint(GetSysMem(), 10))
	pQ.AddTopic("test0", 1000000)

	log.Info("Mem:" + strconv.FormatUint(GetSysMem(), 10))
	pQ.AddTopic("test1", 1000000)
	log.Info("Mem:" + strconv.FormatUint(GetSysMem(), 10))
	go func() {
		fullCnt := 0
		for i := 0; i < 300000; i++ {
			if err := pQ.Write("test0", input); err != nil {
				fmt.Println(fullCnt, err.Error())
				log.Info(strconv.Itoa(fullCnt) + err.Error())
				fullCnt++
			}
		}
		fmt.Println("write 0 exit!******************************************************")
	}()

	go func() {
		fullCnt := 0
		for i := 0; i < 300000; i++ {
			if err := pQ.Write("test1", input); err != nil {
				fmt.Println(fullCnt, err.Error())
				log.Info(strconv.Itoa(fullCnt) + err.Error())
				fullCnt++
			}
		}
		fmt.Println("write 1 exit!******************************************************")
	}()

	time.Sleep(time.Millisecond * 5)

	isExit := 0
	go func() {
		for {
			if isExit > 0 {
				break
			}
			pQ.ReadBatch("test0", 25)
		}
	}()

	go func() {
		for {
			if isExit > 0 {
				break
			}
			pQ.ReadBatch("test1", 25)
		}
	}()

	go func() {
		for {
			if isExit > 0 {
				break
			}
			for _, val := range pQ.PrintQ() {
				fmt.Println(val)
				log.Info(val)
			}
			time.Sleep(time.Second)
		}
	}()

	for {
		if pQ.GetValidLen("test0") == 0 && pQ.GetValidLen("test1") == 0 {
			isExit++
			break
		}
		time.Sleep(time.Millisecond * 1000)
	}
	for _, val := range pQ.PrintQ() {
		fmt.Println(val)
		log.Info(val)
	}
}

func GetSysMem() uint64 {
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	return memStat.Alloc
}
