package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/igorsbezerra/pfa-go/internal/order/infra/database"
	"github.com/igorsbezerra/pfa-go/internal/order/usecase"
	"github.com/igorsbezerra/pfa-go/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	maxWorkers := 3
	wg := sync.WaitGroup{}
	db, err := sql.Open("mysql", "root:root@tcp(mysql:3306)/orders")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	repository := database.NewOrderRepository(db)
	uc := usecase.NewCalculateFinalPriceUseCase(repository)
	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	output := make(chan amqp.Delivery)
	go rabbitmq.Consume(ch, output)

	wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		defer wg.Done()
		go worker(output, uc, i)
	}
	wg.Wait()
}

func worker(deliveryMessage <-chan amqp.Delivery, uc *usecase.CalculateFinalPriceUseCase, workerId int) {
	for msg := range deliveryMessage {
		var input usecase.OrderInputDTO
		err := json.Unmarshal(msg.Body, &input)
		if err != nil {
			fmt.Println("Error unmarshal message", err)
		}
		input.Tax = 10.0
		_, err = uc.Execute(input)
		if err != nil {
			fmt.Println("Error execute usecase", err)
		}
		msg.Ack(false)
		fmt.Println("Worker", workerId, "processed order", input.ID)
	}
}
