package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
)

var addr string

func TestMain(m *testing.M) {
	s, err := NewServer("127.0.0.1:0")
	if err != nil {
		fmt.Printf("Failed to create server: %s\n", err)
		os.Exit(1)
	}

	go s.Run()

	addr = s.listener.Addr().String()

	exitCode := m.Run()

	s.Close()
	os.Exit(exitCode)
}

func setupDial(t *testing.T) (conn net.Conn, scanner *bufio.Scanner) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Fail to dial server: %s", err)
	}

	return conn, bufio.NewScanner(conn)
}

func setupUsername(t *testing.T, conn net.Conn, scanner *bufio.Scanner, username string) {
	t.Helper()
	if !scanner.Scan() {
		t.Fatalf("Failed to read prompt: %s", scanner.Err())
	}

	_, err := conn.Write([]byte(username + "\n"))
	if err != nil {
		t.Fatalf("Failed to write username: %s", err)
	}
}

func TestUsernamePrompt(t *testing.T) {
	conn1, scanner1 := setupDial(t)
	defer conn1.Close()

	if !scanner1.Scan() {
		t.Fatalf("Failed to read prompt: %s", scanner1.Err())
	}

	prompt := scanner1.Text()
	if !strings.Contains(prompt, "username") {
		t.Fatalf("Prompt does not contain 'username': %s", prompt)
	}

	_, err := conn1.Write([]byte("user1\n"))
	if err != nil {
		t.Fatalf("Failed to write username: %s", err)
	}

}

func TestMessageBroadcast(t *testing.T) {
	conn1, scanner1 := setupDial(t)
	defer conn1.Close()

	setupUsername(t, conn1, scanner1, "user1")

	conn2, scanner2 := setupDial(t)
	defer conn2.Close()

	setupUsername(t, conn2, scanner2, "user2")

	broadcastMessage := "Hello world."
	_, err := conn1.Write([]byte(broadcastMessage + "\n"))
	if err != nil {
		t.Fatalf("Client 1 failed to send message: %s", err)
	}

	if !scanner2.Scan() {
		t.Fatalf("Client 2 failed to read broadcast message: %s", scanner2.Err())
	}

	receivedMessage := scanner2.Text()
	expectedMessage := "user1: " + broadcastMessage

	if receivedMessage != expectedMessage {
		t.Fatalf("Expected message %q, but got %q", expectedMessage, receivedMessage)
	}
}

func TestCommands(t *testing.T) {
	conn, scanner := setupDial(t)
	defer conn.Close()

	setupUsername(t, conn, scanner, "user1")

	t.Run("Testing /help command", func(t *testing.T) {
		_, err := conn.Write([]byte("/help\n"))
		if err != nil {
			t.Errorf("Failed to send command /help: %s", err)
		}

		if !scanner.Scan() {
			t.Errorf("Failed to read prompt: %s", scanner.Err())
		}

		helpCommand := scanner.Text()
		if !strings.Contains(helpCommand, "Displays a list of available commands") {
			t.Errorf("Failed to display /help command: %s", helpCommand)
		}

		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "\033[0m") {
				break
			}
		}
	})

	t.Run("Testing /users command", func(t *testing.T) {
		_, err := conn.Write([]byte("/users\n"))
		if err != nil {
			t.Errorf("Failed to send command /users: %s", err)
		}

		if !scanner.Scan() {
			t.Errorf("Failed to read prompt: %s", scanner.Err())
		}

		usersCommand := scanner.Text()
		if !strings.Contains(usersCommand, "Connected users:") {
			t.Errorf("Failed to display /users command: %s", usersCommand)
		}

		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "\033[0m") {
				break
			}
		}
	})

	t.Run("Testing /username command", func(t *testing.T) {
		_, err := conn.Write([]byte("/username newUsername\n"))
		if err != nil {
			t.Errorf("Failed to send command /username: %s", err)
		}

		if !scanner.Scan() {
			t.Errorf("Failed to read prompt: %s", scanner.Err())
		}

		usernameCommand := scanner.Text()
		if !strings.Contains(usernameCommand, "Your username has been changed") {
			t.Errorf("Failed to display /username command: %s", usernameCommand)
		}
	})

	t.Run("Testing /quit command", func(t *testing.T) {
		_, err := conn.Write([]byte("/quit\n"))
		if err != nil {
			t.Errorf("Failed to send command /username: %s", err)
		}

		_, err = conn.Read(make([]byte, 1))
		if err == nil {
			t.Fatal("Failed to quit")
		}
	})
}
