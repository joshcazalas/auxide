package bot

import (
	"fmt"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func HandleShuffleCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID

	var mode string
	for _, option := range i.ApplicationCommandData().Options {
		if option.Name == "mode" {
			mode = option.StringValue()
			break
		}
	}

	var enable bool
	switch strings.ToLower(mode) {
	case "enabled":
		enable = true
	case "disabled":
		enable = false
	default:
		s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "❌ Invalid shuffle mode. Choose `enabled` or `disabled`.",
			},
		})
		return
	}

	GlobalQueue.SetShuffle(channelID, enable)

	status := "disabled"
	if enable {
		status = "enabled"
	}

	embed := &discordgo.MessageEmbed{
		Title:       "🔀 Shuffle Mode Updated",
		Description: fmt.Sprintf("Shuffle mode is now **%s**.", status),
		Color:       0x1DB954,
	}

	s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}

func HandleShuffleAutocomplete(s *discordgo.Session, i *discordgo.InteractionCreate) {
	choices := []*discordgo.ApplicationCommandOptionChoice{
		{Name: "enabled", Value: "enabled"},
		{Name: "disabled", Value: "disabled"},
	}

	s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionApplicationCommandAutocompleteResult,
		Data: &discordgo.InteractionResponseData{
			Choices: choices,
		},
	})
}
