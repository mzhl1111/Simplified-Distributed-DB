package client

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"net/http"
	"os"
)

func main() {
	mqUser := os.Getenv("MQ_USER")
	mqPass := os.Getenv("MQ_PASS")
	mqHost := os.Getenv("MQ_HOST")
	//mqUser := "guest"
	//mqPass := "guest"
	//mqHost := "localhost"

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:5672/", mqUser, mqPass, mqHost))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	// start the lamport timer
	idStr := os.Getenv("Server")

	tsAllocateClient := NewRabbitMQClient(conn, "example-timestamp", "tsResp")
	createClient := NewRabbitMQClient(conn, "newFile", "newFileResp")
	updateClient := NewRabbitMQClient(conn, "updateFile", "updateFileResp")
	deleteClient := NewRabbitMQClient(conn, "deleteFile", "deleteFileResp")
	queryClient := NewRabbitMQClient(conn, "queryFile", "queryFileResp")

	router := gin.Default()

	router.POST("/create/:id", func(c *gin.Context) {
		// once we receive a request, there is an event
		timestamp, err := tsAllocateClient.RequestTSO(idStr)
		if err != nil {
			c.JSON(500, gin.H{"error": "fail to get Ts"})
			return
		}

		id := c.Param("id")
		var forceFail ForceFailRequestBody
		if err := c.BindJSON(&forceFail); err != nil {
			// DO SOMETHING WITH THE ERROR
		}
		msg := CreateMsg{
			BaseMsg: BaseMsg{timestamp, forceFail.forceFail},
			fileID:  id,
		}
		msgJson, err := json.Marshal(msg)
		if err != nil {
			c.JSON(500, gin.H{"error": "message marshal error"})
			return
		}
		err = createClient.sendMsg(msgJson)
		if err != nil {
			c.JSON(500, gin.H{"error": "send Message Error"})
			return
		}
		log.Printf("send msg Lamport timestamp: %d\n", timestamp)
		c.JSON(http.StatusOK, gin.H{"message": "success"})
	})

	router.POST("/update/:id", func(c *gin.Context) {
		// once we receive a request, there is an event
		timestamp, err := tsAllocateClient.RequestTSO(idStr)
		if err != nil {
			c.JSON(500, gin.H{"error": "fail to get Ts"})
			return
		}

		id := c.Param("id")
		var forceFail ForceFailRequestBody
		if err := c.BindJSON(&forceFail); err != nil {
			// DO SOMETHING WITH THE ERROR
		}
		msg := UpdateMsg{
			BaseMsg: BaseMsg{timestamp, forceFail.forceFail},
			fileID:  id,
		}
		msgJson, err := json.Marshal(msg)
		if err != nil {
			c.JSON(500, gin.H{"error": "message marshal error"})
			return
		}
		err = updateClient.sendMsg(msgJson)
		if err != nil {
			c.JSON(500, gin.H{"error": "send Message Error"})
			return
		}
		log.Printf("send msg Lamport timestamp: %d\n", timestamp)
		c.JSON(http.StatusOK, gin.H{"message": "success"})
	})

	router.POST("/delete/:id", func(c *gin.Context) {
		timestamp, err := tsAllocateClient.RequestTSO(idStr)
		if err != nil {
			c.JSON(500, gin.H{"error": "fail to get Ts"})
			return
		}

		id := c.Param("id")
		var forceFail ForceFailRequestBody
		if err := c.BindJSON(&forceFail); err != nil {
			// DO SOMETHING WITH THE ERROR
		}
		msg := UpdateMsg{
			BaseMsg: BaseMsg{timestamp, forceFail.forceFail},
			fileID:  id,
		}
		msgJson, err := json.Marshal(msg)
		if err != nil {
			c.JSON(500, gin.H{"error": "message marshal error"})
			return
		}
		err = deleteClient.sendMsg(msgJson)
		if err != nil {
			c.JSON(500, gin.H{"error": "send Message Error"})
			return
		}
		log.Printf("send msg Lamport timestamp: %d\n", timestamp)
		c.JSON(http.StatusOK, gin.H{"message": "success"})
	})
	router.Run(":3333")
}
