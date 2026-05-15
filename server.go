package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const (
	PORT = ":9000"

	// Colors
	RESET = "\033[0m"
	RED   = "\033[31m"
)

var (
	clients = make(map[net.Conn]Client)
	mu      sync.Mutex
)

type Client struct {
	conn     net.Conn
	username string
}

func main() {
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	defer listener.Close()
	log.Printf("[INFO] Server is running on %s\n", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error: %s", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error: %s", err)
		}

		mu.Lock()
		delete(clients, conn)
		mu.Unlock()
	}(conn)

	connAddr := conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)

	username, err := setUsername(conn, reader)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	client := Client{conn, username}

	mu.Lock()
	clients[conn] = client
	mu.Unlock()

	log.Printf("[INFO] [CONNECTION] New connection established from %s\n", connAddr)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[INFO] [CONNECTION] %s (%s) has closed the connection \n", client.username, client.conn.RemoteAddr().String())
			} else {
				log.Printf("Error: %s\n", err)
			}

			break
		}

		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}

		broadcast(client, message)
	}
}

func setUsername(conn net.Conn, reader *bufio.Reader) (string, error) {
	var username string

	for {
		_, err := fmt.Fprint(conn, "Enter a username (max 16 characters): ")
		if err != nil {
			return fmt.Sprintf("Error: %s", err), err
		}

		username, err = reader.ReadString('\n')
		username = strings.TrimSpace(username)
		if err != nil {
			return "", err
		}

		if len(username) == 0 || len(username) > 16 {
			_, err = fmt.Fprint(conn, RED+"Invalid username. The username must be between 1 and 16 characters long.\n"+RESET)
			if err != nil {
				return fmt.Sprintf("Error: %s", err), err
			}
			continue
		}
		break
	}

	return username, nil
}

func broadcast(sender Client, message string) {
	mu.Lock()
	clientsBroadcast := make([]Client, 0, len(clients))
	for _, client := range clients {
		clientsBroadcast = append(clientsBroadcast, client)
	}
	mu.Unlock()

	fMessage := fmt.Sprintf("%s: %s\n", sender.username, message)
	bMessage := []byte(fMessage)
	for _, client := range clientsBroadcast {
		go func(client Client, bMessage []byte) {
			_, err := client.conn.Write(bMessage)
			if err != nil {
				log.Printf("Error: %s", err)
			}
		}(client, bMessage)
	}

	log.Printf("[DATA] [BROADCAST] [%s]: %s\n", sender.username, message)
}
