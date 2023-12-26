package main

import (
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"sync"
	"time"
)

// 创建全局mqtt publish消息处理 handler
var messagePubHandler = func(client mqtt.Client, msg Message) {
	var cr mqtt.ClientOptionsReader = client.OptionsReader()
	fmt.Printf("Pub Client %s Topic : %s \n", cr.ClientID(), msg.Topic)
	fmt.Printf("Pub Client %s msg : %s \n", cr.ClientID(), msg.Payload)
}

// 创建全局mqtt sub消息处理 handler
var messageSubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	var cr mqtt.ClientOptionsReader = client.OptionsReader()
	fmt.Printf("Sub Client %s Topic : %s  ", cr.ClientID(), msg.Topic())
	fmt.Printf("Sub Client %s  msg : %s \n", cr.ClientID(), msg.Payload())
}

type Message struct {
	Topic   string
	Payload string
}

type message struct {
	topic   string
	payload interface{}
}

// 客户端管理器
type MqttClientManger struct {
	client   mqtt.Client
	msgSend  chan message
	topicSub []string
}

func newMqttClient(options *mqtt.ClientOptions) mqtt.Client {
	client := mqtt.NewClient(options)
	return client
}

func main() {
	go client1()
	time.Sleep(3 * time.Second)
	go client2()
	wg := sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}

func client1() {
	fmt.Println("start1")
	clientManager := NewMqttClient("127.0.0.1", "root", "root", 1883)
	_ = clientManager
	clientManager.NewConnect()
	topic := "topic-test-1"
	clientManager.topicSub = make([]string, 0)
	clientManager.topicSub = append(clientManager.topicSub, topic)
	clientManager.run()
	time.Sleep(1 * time.Second)
	clientManager.msgSend <- message{topic: topic, payload: "message-----1"}
	wg := sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}

func client2() {
	fmt.Println("start2")
	clientManager := NewMqttClient("127.0.0.1", "root", "root", 1883)
	_ = clientManager
	clientManager.NewConnect()
	topic := "topic-test-1"
	clientManager.topicSub = make([]string, 0)
	clientManager.topicSub = append(clientManager.topicSub, topic)
	clientManager.run()
	time.Sleep(1 * time.Second)
	clientManager.msgSend <- message{topic: topic, payload: "message-----2"}
	wg := sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}

func NewMqttClient(ip, username, passd string, port int) *MqttClientManger {
	clinetOptions := mqtt.NewClientOptions().AddBroker(fmt.Sprintf("tcp://%s:%d", ip, port)).SetUsername(fmt.Sprintf("%s", username)).SetPassword(fmt.Sprintf("%s", passd))
	//设置客户端ID
	clinetOptions.SetClientID(ip + "-" + fmt.Sprintf("%d", time.Now().Unix()))
	//设置handler
	//clinetOptions.SetDefaultPublishHandler(messagePubHandler)

	//设置连接超时
	clinetOptions.SetConnectTimeout(time.Duration(60) * time.Second)
	//设置自动重连
	clinetOptions.SetAutoReconnect(true)
	//clinetOptions.SetProtocolVersion(3)
	//创建客户端连接
	c := newMqttClient(clinetOptions)
	msg := make(chan message)
	return &MqttClientManger{client: c, msgSend: msg}
}

// 客户端连接
func (mg *MqttClientManger) NewConnect() {
	if token := mg.client.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("[Pub] mqtt connect error, error: %s \n", token.Error())
		return
	}
}

// 发送消息
func (mg *MqttClientManger) Publish() {
	for {
		msg, ok := <-mg.msgSend
		if ok {
			// 格式化数据，将信息转换为json
			payload, err := json.Marshal(msg.payload)
			//fmt.Printf(string(payload))
			if err != nil {
				fmt.Println(err)
			}
			token := mg.client.Publish(msg.topic, 1, false, payload)
			messagePubHandler(mg.client, Message{Topic: msg.topic, Payload: string(payload)})
			token.Wait()
		}

	}

}

// 订阅消息
func (mg *MqttClientManger) Subscribe() {
	for _, topic := range mg.topicSub {
		token := mg.client.Subscribe(topic, 1, messageSubHandler)
		token.Wait()
		//fmt.Println("sub:",topic)
	}

}

// 启动服务
func (mg *MqttClientManger) run() {
	go mg.Subscribe()
	go mg.Publish()
}
