package main

func main() {
	s, err := NewServer("127.0.0.1:9000")
	if err != nil {
		panic(err)
	}

	s.Run()
}
