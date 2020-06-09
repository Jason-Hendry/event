package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"net/http"
	"net/http/httputil"
)

type Broker struct {

	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan []byte

	// New client connections
	newClients chan chan []byte

	// Closed client connections
	closingClients chan chan []byte

	// Client connections registry
	clients map[chan []byte]bool
}

func (b *Broker) Run() {
	for {
		select {
		case c := <-b.newClients:
			log.Print("New Client")
			b.clients[c] = true
			b.Notifier <- []byte("New Client")
			break
		case c := <-b.closingClients:
			delete(b.clients, c)
			log.Print("Client Gone")
			b.Notifier <- []byte("Client Gone")
			break
		case notice := <-b.Notifier:
			log.Print("Notify")
			for c, _ := range b.clients {
				c <- notice
			}
		}
	}
}

func kafkaConnect(b *Broker) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"HTTP404"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			b.Notifier <- []byte(fmt.Sprintf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value)))
			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func main() {

	broker := &Broker{
		Notifier:       make(chan []byte, 100),
		newClients:     make(chan chan []byte),
		closingClients: make(chan chan []byte),
		clients:        map[chan []byte]bool{},
	}
	go broker.Run()

	go kafkaConnect(broker)

	log.Print("http://localhost:3793")
	http.HandleFunc("/msg", func(w http.ResponseWriter, r *http.Request) {
		broker.Notifier <- []byte(r.Form.Get("msg"))
		fmt.Fprint(w, "Thanks")
	})

	//go func() {
	//	c := 0
	//	for {
	//		<-time.After(time.Second)
	//		// EventSource requires a field name for every line, The field name is either event:  or data:
	//		broker.Notifier <- []byte("Tick\nid: " + fmt.Sprintf("%d", c))
	//		c++
	//	}
	//}()

	http.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		d, _ := httputil.DumpRequest(r, true)
		log.Print(string(d))

		flusher, ok := w.(http.Flusher)

		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(200)
		flusher.Flush()

		// Each connection registers its own message channel with the Broker's connections registry
		messageChan := make(chan []byte)

		defer func() {
			broker.closingClients <- messageChan
		}()
		// Signal the broker that we have a new connection
		broker.newClients <- messageChan

		notify := r.Context().Done()
		go func() {
			<-notify
			broker.closingClients <- messageChan
		}()

		for {
			// Write to the ResponseWriter
			// Server Sent Events compatible
			_, _ = fmt.Fprintf(w, "data: %s\n\n", <-messageChan)

			// Flush the data immediatly instead of buffering it for later.
			flusher.Flush()
		}

	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Print("Static")
		w.Write([]byte(`<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width, user-scalable=no, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>Document</title>
</head>
<body>
<button id="send">Send</button>

<pre id="events"></pre>

<script language="JavaScript">
    var send = document.getElementById("send")
	send.addEventListener('click', function() {
	  fetch('/send');
	});
    var pre = document.getElementById("events")
    var e = new EventSource("/event")
    e.onmessage = (m) => {
        pre.innerText = pre.innerText + "\ndata: "+m.data
    }
</script>
</body>
</html>`))
	})
	http.ListenAndServe(":3793", nil)
}
