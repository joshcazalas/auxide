package main

import (
	"log"
	"os"

	bot "example.com/hello_world_bot/Bot"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file: %v", err)
	}
	bot.BotToken = os.Getenv("DISCORD_BOT_TOKEN")
	bot.Run()
}
