package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

type Action int

const (
	PORT = ":9000"

	// Colors
	RESET = "\033[0m"
	RED   = "\033[31m"

	Register Action = iota
	Unregister
	Broadcast
)

type Server struct {
	listener net.Listener
	event    chan Event
}

type Client struct {
	conn     net.Conn
	username string
	messages chan []byte
}

type Event struct {
	client  *Client
	action  Action
	payload string
}

func NewServer() *Server {
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	log.Printf("[INFO] Server is running on %s\n", listener.Addr())

	return &Server{listener, make(chan Event)}
}

func (s *Server) run() {
	go s.chat()
	s.accept()
}

func (s *Server) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Error: %s", err)
			continue
		}

		go s.handleConnection(conn, s.event)
	}
}

func (s *Server) chat() {
	clients := make(map[net.Conn]Client)
	for msg := range s.event {
		switch msg.action {
		case Unregister:
			delete(clients, msg.client.conn)
			log.Printf("[INFO] [CONNECTION] %s (%s) has closed the connection \n", msg.client.username, msg.client.conn.RemoteAddr().String())
		case Register:
			clients[msg.client.conn] = *msg.client
			log.Printf("[INFO] [CONNECTION] New connection established from %s\n", msg.client.conn.RemoteAddr().String())
		case Broadcast:
			fMessage := fmt.Sprintf("%s: %s\n", msg.client.username, msg.payload)
			bMessage := []byte(fMessage)
			for _, client := range clients {
				select {
				case client.messages <- bMessage:
				default:
					err := client.conn.Close()
					if err != nil {
						log.Printf("Connection close error: %s", err)
					}
					delete(clients, client.conn)
				}
			}

			log.Printf("[DATA] [BROADCAST] [%s]: %s\n", msg.client.username, msg.payload)
		default:
			log.Printf("[ERROR] Invalid action")
		}

	}
}

func (s *Server) handleConnection(conn net.Conn, event chan Event) {
	reader := bufio.NewReader(conn)

	username, err := setUsername(conn, reader)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	client := &Client{conn, username, make(chan []byte)}
	event <- Event{client, Register, ""}

	defer func(conn net.Conn) {
		event <- Event{client, Unregister, ""}
		err := conn.Close()
		if err != nil {
			log.Printf("Error: %s", err)
		}
	}(conn)

	go clientWriter(client)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Printf("Error: %s\n", err)
			break
		}

		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}

		event <- Event{client, Broadcast, message}
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

func clientWriter(client *Client) {
	for message := range client.messages {
		err := client.conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
		if err != nil {
			log.Printf("Write deadline error: %s", err)
		}
		_, err = client.conn.Write(message)
		if err != nil {
			log.Printf("Write error: %s", err)
		}
	}
}
