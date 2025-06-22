package bot

import (
	"github.com/bwmarrin/discordgo"
)

func RegisterSlashCommands(discord *discordgo.Session) {
	commands := []*discordgo.ApplicationCommand{
		{Name: "help", Description: "Get help information"},
		{Name: "bye", Description: "Say goodbye to the bot"},
		{Name: "ping", Description: "Ping the bot"},
		{
			Name:        "play",
			Description: "Enter a song name for the bot to search for",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Type:        discordgo.ApplicationCommandOptionString,
					Name:        "query",
					Description: "Search term",
					Required:    true,
				},
			},
		},
	}

	for _, cmd := range commands {
		_, err := discord.ApplicationCommandCreate(discord.State.User.ID, "", cmd)
		CheckNilErr(err)
	}
}
