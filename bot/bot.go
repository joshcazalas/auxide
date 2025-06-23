package bot

import (
	"log"
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"
)

var BotToken string
var ErrorChan = make(chan error)

func Run() {
	if BotToken == "" {
		log.Fatal("BotToken is empty. Please provide a valid bot token.")
		return
	}

	discord, err := discordgo.New("Bot " + BotToken)
	CheckNilErr(err)

	RegisterSlashCommands(discord)
	RegisterComponentHandlers()
	RegisterAutocompleteHandlers()

	discord.AddHandler(Message)
	discord.AddHandler(Interaction)

	err = discord.Open()
	CheckNilErr(err)
	defer discord.Close()

	log.Println("Bot running...")

	go func() {
		for err := range ErrorChan {
			log.Printf("Received error from bot: %v", err)
			channelID, chErr := GetTextChannel(discord)
			if chErr != nil {
				log.Printf("Failed to find text channel to send error message: %v", chErr)
				continue
			}

			embed := &discordgo.MessageEmbed{
				Title:       "⚠️ Bot Error",
				Description: err.Error(),
				Color:       0xE03C3C,
			}

			_, sendErr := discord.ChannelMessageSendEmbed(channelID, embed)
			if sendErr != nil {
				log.Printf("Failed to send error message to channel %s: %v", channelID, sendErr)
			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	close(ErrorChan)
}
