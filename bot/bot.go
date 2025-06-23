package bot

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"
)

var BotToken string
var ErrorChan = make(chan GuildError)

const BotTextChannelName = "music-bot-channel"

var botTextChannels = make(map[string]string)

type GuildError struct {
	GuildID string
	Err     error
}

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
		for guildErr := range ErrorChan {
			log.Printf("Bot error in guild %s: %v", guildErr.GuildID, guildErr.Err)

			channelID, ok := botTextChannels[guildErr.GuildID]
			if !ok {
				log.Printf("No bot text channel recorded for guild %s to send error message", guildErr.GuildID)
				continue
			}

			embed := &discordgo.MessageEmbed{
				Title:       "⚠️ Bot Error",
				Description: guildErr.Err.Error(),
				Color:       0xE03C3C,
			}

			_, sendErr := discord.ChannelMessageSendEmbed(channelID, embed)
			if sendErr != nil {
				log.Printf("Failed to send error message to channel %s in guild %s: %v", channelID, guildErr.GuildID, sendErr)
			}
		}
	}()

	err = InitializeBotChannels(discord)
	if err != nil {
		log.Printf("Failed to initialize bot channels: %v", err)
		ErrorChan <- GuildError{
			GuildID: discord.State.Application.GuildID,
			Err:     fmt.Errorf("failed to initialize bot channels: %v", err),
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	close(ErrorChan)
}
