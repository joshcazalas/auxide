package bot

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/bwmarrin/discordgo"
)

var BotToken string
var ErrorChan = make(chan GuildError)

const BotTextChannelName = "music-bot-channel"
const CacheDir = "/tmp/discordmusicbot"
const cleanupFrequency = 1 * time.Hour
const maxFileAge = 6 * time.Hour

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

	ready := make(chan struct{})
	discord.AddHandlerOnce(func(s *discordgo.Session, r *discordgo.Ready) {
		close(ready)
	})

	RegisterComponentHandlers()
	RegisterAutocompleteHandlers()

	discord.AddHandler(Message)
	discord.AddHandler(Interaction)

	err = discord.Open()
	CheckNilErr(err)
	defer discord.Close()

	<-ready

	botID := discord.State.User.ID
	for _, guild := range discord.State.Guilds {
		existingCmds, err := discord.ApplicationCommands(botID, guild.ID)
		CheckNilErr(err)

		existingNames := make(map[string]*discordgo.ApplicationCommand)
		for _, cmd := range existingCmds {
			existingNames[cmd.Name] = cmd
		}

		configNames := make(map[string]bool)
		for _, cmd := range SlashCommands {
			configNames[cmd.Command.Name] = true
		}

		for name, cmd := range existingNames {
			if !configNames[name] {
				err := discord.ApplicationCommandDelete(botID, guild.ID, cmd.ID)
				if err != nil {
					log.Printf("Failed to delete stale command /%s for guild %s: %v", name, guild.ID, err)
				} else {
					log.Printf("Deleted stale command /%s for guild %s", name, guild.ID)
				}
			}
		}

		for _, cmd := range SlashCommands {
			if _, exists := existingNames[cmd.Command.Name]; exists {
				continue
			}
			_, err := discord.ApplicationCommandCreate(botID, guild.ID, cmd.Command)
			if err != nil {
				log.Printf("Failed to create command /%s for guild %s: %v", cmd.Command.Name, guild.ID, err)
			} else {
				log.Printf("Registered /%s for guild %s", cmd.Command.Name, guild.ID)
			}
		}
	}

	StartCleanupRoutine(CacheDir, cleanupFrequency, maxFileAge)

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

	log.Println("Bot running...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	close(ErrorChan)
}
