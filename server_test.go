package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

func setupDial(t *testing.T) func() net.Conn {
	t.Helper()

	s, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create server: %s", err)
	}

	go s.Run()

	t.Cleanup(func() {
		s.Close()
	})

	return func() net.Conn {
		conn, err := net.Dial("tcp", s.listener.Addr().String())
		if err != nil {
			t.Fatalf("Fail to dial server: %s", err)
		}

		return conn
	}
}

func TestUsernamePrompt(t *testing.T) {
	dial := setupDial(t)
	conn1 := dial()
	defer conn1.Close()

	scanner := bufio.NewScanner(conn1)
	if !scanner.Scan() {
		t.Fatalf("Failed to read prompt: %s", scanner.Err())
	}

	prompt := scanner.Text()
	if !strings.Contains(prompt, "username") {
		t.Fatalf("Prompt does not contain 'username': %s", prompt)
	}

	_, err := conn1.Write([]byte("user1\n"))
	if err != nil {
		t.Fatalf("Failed to write username: %s", err)
	}

}
