package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	kaf "kafkapub/pkg"
	"kafkapub/pkg/kafkianos"

	"github.com/gin-gonic/gin"
)

type Request struct {
	Username string `json:"username"`
	Message  string `json:"message"`
}

func main() {

	var (
		brokers = os.Getenv("KAFKA_BROKERS")
		topic   = os.Getenv("KAFKA_TOPIC")
	)

	brokersList := strings.Split(brokers, ",")
	publisher := kafkianos.NewPublisher(brokersList, topic)

	r := gin.Default()
	r.POST("/join", joinHandler(publisher))
	r.POST("/publish", publishHandler(publisher))

	_ = r.Run()
}

func joinHandler(publisher kaf.Publisher) func(*gin.Context) {
	return func(c *gin.Context) {
		var req Request
		err := json.NewDecoder(c.Request.Body).Decode(&req)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
		}

		message := kaf.NewSystemMessage(fmt.Sprintf("%s has joined the room!", req.Username))

		if err := publisher.Publish(context.Background(), message); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
		}

		c.JSON(http.StatusAccepted, gin.H{"message": "message published"})
	}
}

func publishHandler(publisher kaf.Publisher) func(*gin.Context) {
	return func(c *gin.Context) {
		var req Request
		err := json.NewDecoder(c.Request.Body).Decode(&req)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
		}

		message := kaf.NewMessage(req.Username, req.Message)

		if err := publisher.Publish(context.Background(), message); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
		}

		c.JSON(http.StatusAccepted, gin.H{"message": "message published"})
	}
}
