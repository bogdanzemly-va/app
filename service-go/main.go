package main

import (
"context"
"encoding/json"
"log"
"net/http"
"fmt"
	"os"
"time"

"github.com/gin-gonic/gin"
"github.com/redis/go-redis/v9"
"github.com/streadway/amqp"
"go.mongodb.org/mongo-driver/bson"
"go.mongodb.org/mongo-driver/mongo"
"go.mongodb.org/mongo-driver/mongo/options"
"github.com/IBM/sarama"
)

type Shipment struct {
OrderID   string    `json:"order_id" bson:"order_id"`
Status    string    `json:"status" bson:"status"`
CreatedAt time.Time `json:"created_at" bson:"created_at"`
}

var (
mongoClient *mongo.Client
rabbitChan  *amqp.Channel
redisClient *redis.Client
)

func main() {
mongoURI  := getEnv("MONGO_URI", "mongodb://mongodb:27017")
rabbitURL := getEnv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
redisAddr := getEnv("REDIS_ADDR", "redis:6379")
kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")

ctx := context.Background()
var err error

mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
if err != nil {
log.Fatalf("MongoDB connect error: %v", err)
}
log.Println("MongoDB connected")

conn, err := amqp.Dial(rabbitURL)
if err != nil {
log.Fatalf("RabbitMQ connect error: %v", err)
}
rabbitChan, err = conn.Channel()
if err != nil {
log.Fatalf("RabbitMQ channel error: %v", err)
}
_, err = rabbitChan.QueueDeclare("shipment.ready", true, false, false, false, nil)
if err != nil {
log.Fatalf("Queue declare error: %v", err)
}
log.Println("RabbitMQ connected")

redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})
log.Println("Redis connected")

go startKafkaConsumer(kafkaBroker)

router := gin.Default()
router.GET("/healthz", func(c *gin.Context) {
c.JSON(http.StatusOK, gin.H{"status": "ok"})
})
router.GET("/shipments/:order_id", getShipment)
router.POST("/shipments", createShipment)

log.Println("Listening on :8080")
log.Fatal(router.Run(":8080"))
}

func getShipment(c *gin.Context) {
orderID := c.Param("order_id")
ctx := context.Background()
cacheKey := "shipment:" + orderID

cached, err := redisClient.Get(ctx, cacheKey).Result()
if err == nil {
var shipment Shipment
json.Unmarshal([]byte(cached), &shipment)
log.Printf("Cache hit for shipment %s", orderID)
c.JSON(http.StatusOK, shipment)
return
}

collection := mongoClient.Database("shipments").Collection("shipments")
var shipment Shipment
err = collection.FindOne(ctx, bson.M{"order_id": orderID}).Decode(&shipment)
if err != nil {
if err == mongo.ErrNoDocuments {
c.JSON(http.StatusNotFound, gin.H{"error": "shipment not found"})
} else {
c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}
return
}

data, _ := json.Marshal(shipment)
redisClient.SetEx(ctx, cacheKey, string(data), 30*time.Second)
log.Printf("Cache miss for shipment %s, stored in Redis", orderID)

c.JSON(http.StatusOK, shipment)
}

func createShipment(c *gin.Context) {
var body map[string]string
if err := c.ShouldBindJSON(&body); err != nil {
c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
return
}
orderID := body["order_id"]
if orderID == "" {
c.JSON(http.StatusBadRequest, gin.H{"error": "order_id required"})
return
}
saveShipment(orderID)
c.JSON(http.StatusCreated, gin.H{"order_id": orderID, "status": "pending"})
}

func saveShipment(orderID string) {
shipment := Shipment{
OrderID:   orderID,
Status:    "pending",
CreatedAt: time.Now(),
}
collection := mongoClient.Database("shipments").Collection("shipments")
_, err := collection.InsertOne(context.Background(), shipment)
if err != nil {
log.Printf("Failed to insert shipment: %v", err)
return
}

event := map[string]string{"order_id": orderID, "shipment_id": orderID}
body, _ := json.Marshal(event)
rabbitChan.Publish("", "shipment.ready", false, false, amqp.Publishing{
ContentType: "application/json",
Body:        body,
})
log.Printf("Shipment created for order %s", orderID)
}

func startKafkaConsumer(broker string) {
config := sarama.NewConfig()
config.Consumer.Offsets.Initial = sarama.OffsetOldest
config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

var consumer sarama.ConsumerGroup
var err error
for i := 0; i < 10; i++ {
consumer, err = sarama.NewConsumerGroup([]string{broker}, "service-go", config)
if err == nil {
break
}
log.Printf("Kafka not ready, retry %d/10: %v", i+1, err)
time.Sleep(5 * time.Second)
}
if err != nil {
log.Printf("Kafka consumer failed, skipping: %v", err)
return
}
defer consumer.Close()
log.Println("Kafka consumer started")

for {
consumer.Consume(context.Background(), []string{"order.created"}, &handler{})
}
}

type handler struct{}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
for msg := range claim.Messages() {
var event map[string]interface{}
if err := json.Unmarshal(msg.Value, &event); err != nil {
continue
}
orderID := ""
switch v := event["order_id"].(type) {
case float64:
orderID = fmt.Sprintf("%d", int(v))
case string:
orderID = v
}
if orderID != "" {
saveShipment(orderID)
}
session.MarkMessage(msg, "")
}
return nil
}

func getEnv(key, def string) string {
if v := os.Getenv(key); v != "" {
return v
}
return def
}
