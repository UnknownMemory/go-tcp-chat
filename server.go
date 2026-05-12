package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

const (
	PORT = ":9000"

	// Colors
	RESET = "\033[0m"
	RED   = "\033[31m"
)

type Connection struct {
	addr     string
	username string
}

var connections = make(map[net.Conn]Connection)

func main() {
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	defer listener.Close()

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
	defer conn.Close()
	connAddr := conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)

	username, err := setUsername(conn, reader)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	connections[conn] = Connection{conn.RemoteAddr().String(), username}

	fmt.Printf("Handling connection: %s\n", connAddr)
	for {
		message, err := reader.ReadString('\n')
		message = strings.TrimSpace(message)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Printf("Connection closed: %s\n", connAddr)
			} else {
				fmt.Printf("Error: %s\n", err)
			}

			break
		}

		fmt.Printf("%s: %s\n", connections[conn].username, message)
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
