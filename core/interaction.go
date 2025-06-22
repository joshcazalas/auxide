package core

import (
	"log"

	"github.com/bwmarrin/discordgo"
	"github.com/joshcazalas/discord-music-bot/cmd"
)

func Interaction(s *discordgo.Session, i *discordgo.InteractionCreate) {
	userID := GetUserID(i)
	if userID == s.State.User.ID {
		return
	}

	if i.Type == discordgo.InteractionApplicationCommand {
		switch i.ApplicationCommandData().Name {
		case "help":
			Respond(s, i, "Here are the available commands: `/help`, `/bye`")
		case "bye":
			Respond(s, i, "Goodbye! See you later.")
		case "ping":
			Respond(s, i, "pong")
		case "play":
			cmd.Play(s, i, userID)
		default:
			log.Println("Unknown command:", i.ApplicationCommandData().Name)
		}
	}
}

func HandleInteraction(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	userID := GetUserID(i)
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
			CheckNilErr(err)

		case "bye":
			// Respond to /bye
			err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "Goodbye! See you later.",
				},
			})
			CheckNilErr(err)

		case "ping":
			// Respond to /ping
			err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "pong",
				},
			})
			CheckNilErr(err)

		case "play":
			cmd.Play(discord, i, userID)

		default:
			log.Println("Unknown command:", i.ApplicationCommandData().Name)
		}
	}
}
