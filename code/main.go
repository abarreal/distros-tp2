package main

import (
	"log"
	"os"
)

func main() {
	// Run the specific node depending on program parameters.
	program := os.Args[1]

	log.Printf("Starting program: %s", program)
	// TODO: Call Run method depending on argument.
}
