package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetInt(key string) (int, error) {
	if str, found := GetString(key); found {
		return strconv.Atoi(str)
	} else {
		return 0, errors.New("not found")
	}
}

func GetIntOrDefault(key string, def int) (int, error) {
	if str, found := GetString(key); found {
		return strconv.Atoi(str)
	} else {
		return def, nil
	}
}

func GetString(key string) (string, bool) {
	return os.LookupEnv(key)
}

func GetStringOrDefault(key string, def string) string {
	val, found := GetString(key)

	if found {
		return val
	} else {
		return def
	}
}

// Set initial environment variables from a configuration file, if they
// have not been defined already.
func UseFile(path string) error {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		// The file does not exist. Assume that file configuration is not
		// intended and just return.
		return nil
	}

	file, err := os.Open(path)

	if err != nil {
		return fmt.Errorf("could not read configuration file: %w", err)
	}

	// Ensure that the file will be closed.
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		// Get current line.
		line := scanner.Text()
		// Split by the character =
		tokenized := strings.Split(line, "=")
		// Get variable name and value.
		key := tokenized[0]
		val := strings.Join(tokenized[1:], "=")
		// Set the environment variable if it has not been defined.
		if _, defined := os.LookupEnv(key); !defined {
			os.Setenv(key, val)
		}
	}

	// Everything went well; return no error.
	return nil
}
