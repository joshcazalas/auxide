package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func Interaction(discord *discordgo.Session, i *discordgo.InteractionCreate) {
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
			Play(discord, i, userID)

		default:
			log.Println("Unknown command:", i.ApplicationCommandData().Name)
		}
	}
}
