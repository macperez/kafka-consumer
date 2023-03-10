package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	kaf "kafkapub/pkg"
	"kafkapub/pkg/kafkianos"
)

type Data struct {
	Username string `json:"username"`
	Message  string `json:"message"`
}

func main() {

	var (
		brokers = os.Getenv("KAFKA_BROKERS")
		topic   = os.Getenv("KAFKA_TOPIC")
	)

	fmt.Println(brokers)
	fmt.Println(topic)

	chMsg := make(chan kaf.Message)
	chErr := make(chan error)
	consumer := kafkianos.NewConsumer(strings.Split(brokers, ","), topic)

	go func() {
		consumer.Read(context.Background(), chMsg, chErr)
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-quit:
			goto end
		case m := <-chMsg:
			printMessage(m)
		case err := <-chErr:
			log.Println(err)
		}
	}
end:

	fmt.Println("\nThe consumer-producer has been finalished")

}

func printMessage(m kaf.Message) {

	switch {
	//case m.Username == user:
	//	return
	case m.Username == kaf.SystemID:
		fmt.Println(m.Message)
	default:
		fmt.Printf("%s say: %s", m.Username, m.Message)
	}
}
