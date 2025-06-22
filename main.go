package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/joshcazalas/discord-music-bot/bot"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	bot.BotToken = os.Getenv("DISCORD_BOT_TOKEN")
	bot.Run()
}
