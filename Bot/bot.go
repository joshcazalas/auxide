package bot

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/bwmarrin/discordgo"
)

var BotToken string

func checkNilErr(e error) {
	if e != nil {
		log.Fatal("Error: %v", e)
	}
}

func Run() {
	if BotToken == "" {
		log.Fatal("BotToken is empty. Please provide a valid bot token.")
		return
	}

	discord, err := discordgo.New("Bot " + BotToken)
	checkNilErr(err)

	discord.AddHandler(newMessage)
	discord.AddHandler(newInteraction)

	err = discord.Open()
	checkNilErr((err))

	registerSlashCommands(discord)

	defer discord.Close()

	fmt.Println("Bot running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func newMessage(discord *discordgo.Session, message *discordgo.MessageCreate) {
	// Stop bot from responding to itself
	if message.Author.ID == discord.State.User.ID {
		return
	}

	switch {
	case strings.Contains(message.Content, "!help"):
		discord.ChannelMessageSend(message.ChannelID, "")
	case strings.Contains(message.Content, "!bye"):
		discord.ChannelMessageSend(message.ChannelID, "Goodbye")
	}
}

func registerSlashCommands(discord *discordgo.Session) {
	_, err := discord.ApplicationCommandCreate(discord.State.User.ID, "", &discordgo.ApplicationCommand{
		Name:        "help",
		Description: "Get help information",
	})
	checkNilErr((err))

	_, err = discord.ApplicationCommandCreate(discord.State.User.ID, "", &discordgo.ApplicationCommand{
		Name:        "bye",
		Description: "Say goodbye to the bot.",
	})
	checkNilErr(err)

	_, err = discord.ApplicationCommandCreate(discord.State.User.ID, "", &discordgo.ApplicationCommand{
		Name:        "ping",
		Description: "Ping the bot.",
	})
	checkNilErr(err)
}

func newInteraction(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	var userID string
	if i.Member != nil {
		userID = i.Member.User.ID // use Member.User.ID for servers
	} else if i.User != nil {
		userID = i.User.ID // use User.ID for DMs
	} else {
		log.Println("Interaction does not contain a User or Member")
		return
	}

	// Stop bot from responding to itself
	if userID == discord.State.User.ID {
		return
	}

	if i.Type == discordgo.InteractionApplicationCommand {
		switch i.ApplicationCommandData().Name {
		case "help":
			// Respond to /help
			err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "Here are the available commands: `/help`, `/bye`",
				},
			})
			checkNilErr(err)

		case "bye":
			// Respond to /bye
			err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "Goodbye! See you later.",
				},
			})
			checkNilErr(err)

		case "ping":
			// Respond to /ping
			err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "pong",
				},
			})
			checkNilErr(err)

		default:
			log.Println("Unknown command:", i.ApplicationCommandData().Name)
		}
	}
}
