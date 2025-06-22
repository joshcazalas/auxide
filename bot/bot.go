package bot

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"
	"github.com/joshcazalas/discord-music-bot/core"
)

var BotToken string

func Run() {
	if BotToken == "" {
		log.Fatal("BotToken is empty. Please provide a valid bot token.")
		return
	}

	discord, err := discordgo.New("Bot " + BotToken)
	core.CheckNilErr(err)

	discord.AddHandler(core.Message)
	discord.AddHandler(core.Interaction)
	discord.AddHandler(core.Component)

	err = discord.Open()
	core.CheckNilErr(err)

	core.RegisterSlashCommands(discord)

	defer discord.Close()

	fmt.Println("Bot running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
