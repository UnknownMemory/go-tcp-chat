package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type Action int

const (
	// Colors
	RESET = "\033[0m"
	RED   = "\033[31m"
	GREEN = "\033[32m"

	Register Action = iota
	Unregister
	Broadcast
	CMD
	Shutdown
)

type Server struct {
	addr     string
	listener net.Listener
	event    chan Event
	ctx      context.Context
	cancel   context.CancelFunc
	serverWg *sync.WaitGroup
	connWg   *sync.WaitGroup
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

type command struct {
	command string
	desc    string
	args    []string
}

var commands = map[string]command{
	"help":     {"help", "Displays a list of available commands", []string{}},
	"username": {"username", "Changes the username of the current user", []string{"username"}},
	"users":    {"users", "Displays a list of connected users", []string{}},
	"quit":     {"quit", "Exits the chat", []string{}},
}

func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Error: %s\n", err)
	}
	log.Printf("[INFO] Server is running on %s\n", listener.Addr())

	serverWg := &sync.WaitGroup{}
	connWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{addr, listener, make(chan Event), ctx, cancel, serverWg, connWg}, nil
}

func (s *Server) Run() {
	s.serverWg.Add(2)
	go s.chat()
	go s.accept()

	s.serverWg.Wait()
}

func (s *Server) accept() {
	defer s.serverWg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("Error: %s", err)
				continue
			}
		}

		s.connWg.Add(1)
		go s.handleConnection(conn, s.event)
	}
}

func (s *Server) Close() {
	s.cancel()
	err := s.listener.Close()
	if err != nil {
		log.Printf("Error closing listener: %s", err)
	}
	s.event <- Event{action: Shutdown}
	s.connWg.Wait()
	close(s.event)

}

func (s *Server) chat() {
	defer s.serverWg.Done()
	clients := make(map[net.Conn]*Client)

	for msg := range s.event {
		switch msg.action {
		case Unregister:
			if _, exists := clients[msg.client.conn]; exists {
				delete(clients, msg.client.conn)
				close(msg.client.messages)
				log.Printf("[INFO] [CONNECTION] %s (%s) has closed the connection \n", msg.client.username, msg.client.conn.RemoteAddr().String())
			} else {
				log.Printf("[INFO] [CONNECTION] %s (%s) closed earlier\n", msg.client.username, msg.client.conn.RemoteAddr().String())
			}
		case Register:
			clients[msg.client.conn] = msg.client
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

				}
			}

			log.Printf("[DATA] [BROADCAST] [%s]: %s\n", msg.client.username, msg.payload)
		case CMD:
			cmd := strings.Split(msg.payload, " ")
			args := cmd[1:]

			switch cmd[0] {
			case "/help":
				for _, c := range commands {
					_, err := fmt.Fprintf(msg.client.conn, GREEN+"%s - %s <%s>\n"+RESET, c.command, c.desc, strings.Join(c.args, ", "))
					if err != nil {
						log.Printf("Error: %s", err)
					}
				}
			case "/username":
				if len(args) != 1 {
					_, err := fmt.Fprintf(msg.client.conn, RED+"Invalid arguments. Usage: /username <new username>\n"+RESET)
					if err != nil {
						log.Printf("Error: %s", err)
					}
					break
				}

				clients[msg.client.conn].username = args[0]

				_, err := fmt.Fprintf(msg.client.conn, GREEN+"Your username has been changed!\n"+RESET)
				if err != nil {
					log.Printf("Error: %s", err)
				}
			case "/users":
				users := make([]string, 0, len(clients))
				for _, client := range clients {
					users = append(users, client.username)
				}
				_, err := fmt.Fprintf(msg.client.conn, GREEN+"Connected users:\n%s\n"+RESET, strings.Join(users, "\n"))
				if err != nil {
					log.Printf("Error: %s", err)
				}
			case "/quit":
				msg.client.conn.Close()
			}

		case Shutdown:
			for _, client := range clients {
				client.conn.Close()
			}
		default:
			log.Printf("[ERROR] Invalid action")
		}
	}
}

func (s *Server) handleConnection(conn net.Conn, event chan Event) {
	defer s.connWg.Done()

	reader := bufio.NewReader(conn)
	username, err := s.setUsername(conn, reader)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	client := &Client{conn, username, make(chan []byte, 16)}
	event <- Event{client, Register, ""}

	s.connWg.Add(1)
	go s.clientWriter(client)

	defer func() {
		event <- Event{client, Unregister, ""}
	}()

	for {
		eventType := Broadcast

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

		if strings.HasPrefix(message, "/") {
			cmd := strings.Split(message, " ")

			if _, exists := commands[strings.TrimPrefix(cmd[0], "/")]; !exists {
				_, err = fmt.Fprintf(conn, RED+"Invalid command. Type /help for a list of available commands.\n"+RESET)
				if err != nil {
					log.Printf("Error: %s", err)
					continue
				}
			}

			eventType = CMD
		}

		select {
		case event <- Event{client, eventType, message}:
		case <-s.ctx.Done():
			return
		}

	}
}

func (s *Server) setUsername(conn net.Conn, reader *bufio.Reader) (string, error) {
	var username string

	for {
		_, err := fmt.Fprint(conn, "Enter a username (max 16 characters):\n")
		if err != nil {
			return "", err
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

func (s *Server) clientWriter(client *Client) {
	defer s.connWg.Done()

	for message := range client.messages {
		_, err := client.conn.Write(message)
		if err != nil {
			log.Printf("Write error: %s", err)
			return
		}
	}

}
