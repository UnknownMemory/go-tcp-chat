package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

func setupDial(t *testing.T) func() (net.Conn, *bufio.Scanner) {
	t.Helper()

	s, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create server: %s", err)
	}

	go s.Run()

	t.Cleanup(func() {
		s.Close()
	})

	return func() (net.Conn, *bufio.Scanner) {
		conn, err := net.Dial("tcp", s.listener.Addr().String())
		if err != nil {
			t.Fatalf("Fail to dial server: %s", err)
		}

		return conn, bufio.NewScanner(conn)
	}
}

func TestUsernamePrompt(t *testing.T) {
	dial := setupDial(t)
	conn1, scanner1 := dial()
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
	dial := setupDial(t)
	conn1, scanner1 := dial()
	defer conn1.Close()

	if !scanner1.Scan() {
		t.Fatalf("Failed to read prompt: %s", scanner1.Err())
	}

	_, err := conn1.Write([]byte("user1\n"))
	if err != nil {
		t.Fatalf("Failed to write username: %s", err)
	}

	conn2, scanner2 := dial()
	defer conn2.Close()

	if !scanner2.Scan() {
		t.Fatalf("Client 2 failed to read prompt: %s", scanner1.Err())
	}

	_, err = conn2.Write([]byte("user2\n"))
	if err != nil {
		t.Fatalf("Client 2 failed to write username: %s", err)
	}

	broadcastMessage := "Hello world."
	_, err = conn1.Write([]byte(broadcastMessage + "\n"))
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
