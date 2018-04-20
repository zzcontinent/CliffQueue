package CliffQueue

import (
	"errors"
	"sync"
	"strconv"
	"git.woda.ink/woda/common/utils/CliffMemSQL"
)

/*
map 在多线程时不能同时读写，运行报错，需要使用结构体解决
*/
type cCliffQueue struct {
	m_Topic  []string
	m_Detail []stTopicQ
}

type stTopicQ struct {
	m_Topic     string
	m_Data      []interface{}
	m_pStart    int
	m_pEnd      int
	m_QMaxLen   int
	m_ValidLen  int //环形缓冲区存储数据长度
	m_MutexLock *sync.Mutex
}

func NewQ() *cCliffQueue {
	newQ := cCliffQueue{}
	newQ.m_Topic = make([]string, 0)
	newQ.m_Detail = make([]stTopicQ, 0)
	return &newQ
}

func (this *cCliffQueue) isTopicIn(inStr string) bool {
	for _, val := range this.m_Topic {
		if val == inStr {
			return true
		}
	}
	return false
}
func (this *cCliffQueue) GetTopicIndex(inStr string) int {
	if !this.isTopicIn(inStr) {
		return -9999
	}
	for i, v := range this.m_Topic {
		if v == inStr {
			return i
		}
	}
	return -9999
}

func (this *cCliffQueue) AddTopic(inTopic string, inLen int) error {
	if this == nil {
		return errors.New("this is nil point")
	}
	if this.isTopicIn(inTopic) {
		return errors.New("Topic already exist")
	}
	if inLen < 0 {
		inLen = 0
	}
	this.m_Topic = append(this.m_Topic, inTopic)
	this.m_Detail = append(this.m_Detail, stTopicQ{
		m_Topic:     inTopic,
		m_MutexLock: &sync.Mutex{},
		m_ValidLen:  0,
		m_QMaxLen:   inLen,
		m_Data:      make([]interface{}, inLen),
		m_pStart:    0,
		m_pEnd:      0,
	})
	return nil
}

func (this *cCliffQueue) Write(inTopic string, inData []interface{}) error {
	topicIndex := this.GetTopicIndex(inTopic)
	if topicIndex < 0 {
		return errors.New("topic not exist")
	}

	this.m_Detail[topicIndex].m_MutexLock.Lock()
	defer this.m_Detail[topicIndex].m_MutexLock.Unlock()

	for i, val := range inData {
		if this.m_Detail[topicIndex].m_ValidLen < this.m_Detail[topicIndex].m_QMaxLen {
			this.m_Detail[topicIndex].m_Data[this.m_Detail[topicIndex].m_pEnd] = val
			this.m_Detail[topicIndex].m_pEnd++
			this.m_Detail[topicIndex].m_pEnd = this.m_Detail[topicIndex].m_pEnd % this.m_Detail[topicIndex].m_QMaxLen
			this.m_Detail[topicIndex].m_ValidLen++
		} else {
			return errors.New("Queue is full, Topic=" + inTopic + " len= " + strconv.Itoa(this.m_Detail[topicIndex].m_QMaxLen) + " start= " + strconv.Itoa(this.m_Detail[topicIndex].m_pStart) + " | " + " inData[" + strconv.Itoa(i) + "]=" + CliffMemSQL.CGetInterface.GetValToString(val))
		}
	}
	return nil
}

func (this *cCliffQueue) Read(inTopic string) (interface{}) {
	topicIndex := this.GetTopicIndex(inTopic)
	if topicIndex < 0 {
		return errors.New("topic not exist")
	}

	this.m_Detail[topicIndex].m_MutexLock.Lock()
	defer this.m_Detail[topicIndex].m_MutexLock.Unlock()

	if this.m_Detail[topicIndex].m_ValidLen > 0 {
		outData := this.m_Detail[topicIndex].m_Data[this.m_Detail[topicIndex].m_pStart]
		this.m_Detail[topicIndex].m_pStart++
		this.m_Detail[topicIndex].m_pStart = this.m_Detail[topicIndex].m_pStart % this.m_Detail[topicIndex].m_QMaxLen
		this.m_Detail[topicIndex].m_ValidLen--
		return outData
	}
	return nil
}

func (this *cCliffQueue) ReadBatch(inTopic string, inCnt int) ([]interface{}) {
	topicIndex := this.GetTopicIndex(inTopic)
	if topicIndex < 0 {
		return make([]interface{}, 0)
	}

	this.m_Detail[topicIndex].m_MutexLock.Lock()
	defer this.m_Detail[topicIndex].m_MutexLock.Unlock()
	outSlice := make([]interface{}, 0)
	for i := 0; i < inCnt; i++ {
		if this.m_Detail[topicIndex].m_ValidLen > 0 {
			outData := this.m_Detail[topicIndex].m_Data[this.m_Detail[topicIndex].m_pStart]
			this.m_Detail[topicIndex].m_pStart++
			this.m_Detail[topicIndex].m_pStart = this.m_Detail[topicIndex].m_pStart % this.m_Detail[topicIndex].m_QMaxLen
			this.m_Detail[topicIndex].m_ValidLen--
			outSlice = append(outSlice, outData)
		}
	}
	return outSlice
}

func (this *cCliffQueue) GetValidLen(inTopic string) int {
	index := this.GetTopicIndex(inTopic)
	this.m_Detail[index].m_MutexLock.Lock()
	defer this.m_Detail[index].m_MutexLock.Unlock()
	return this.m_Detail[index].m_ValidLen
}

func (this *cCliffQueue) PrintQ() []string {
	outStr := make([]string, 0)
	i := 0
	for topicIndex, val := range this.m_Topic {
		this.m_Detail[topicIndex].m_MutexLock.Lock()
		i++
		outStr = append(outStr, strconv.Itoa(i))
		outStr = append(outStr, "topic="+val)
		outStr = append(outStr, "start="+strconv.Itoa(this.m_Detail[topicIndex].m_pStart))
		outStr = append(outStr, "end="+strconv.Itoa(this.m_Detail[topicIndex].m_pEnd))
		outStr = append(outStr, "MaxLen="+strconv.Itoa(this.m_Detail[topicIndex].m_QMaxLen))
		outStr = append(outStr, "ValidLen="+strconv.Itoa(this.m_Detail[topicIndex].m_ValidLen))
		outStr = append(outStr, "------------------------------------------")
		this.m_Detail[topicIndex].m_MutexLock.Unlock()
	}
	return outStr
}
