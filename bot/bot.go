package bot

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"
)

var BotToken string

func Run() {
	if BotToken == "" {
		log.Fatal("BotToken is empty. Please provide a valid bot token.")
		return
	}

	discord, err := discordgo.New("Bot " + BotToken)
	CheckNilErr(err)

	discord.AddHandler(Message)
	discord.AddHandler(Component)
	discord.AddHandler(Interaction)

	err = discord.Open()
	CheckNilErr(err)

	RegisterSlashCommands(discord)

	defer discord.Close()

	fmt.Println("Bot running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
