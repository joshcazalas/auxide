package bot

import (
	"log"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func Interaction(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	userID := GetUserID(i)
	if userID == discord.State.User.ID {
		return
	}

	switch i.Type {

	case discordgo.InteractionApplicationCommand:
		name := i.ApplicationCommandData().Name
		if cmd, ok := SlashCommands[name]; ok {
			cmd.Handler(discord, i)
		} else {
			log.Println("Unknown slash command:", name)
		}

	case discordgo.InteractionMessageComponent:
		customID := i.MessageComponentData().CustomID
		for id, handler := range ComponentHandlers {
			if strings.HasPrefix(customID, id) {
				handler(discord, i)
				return
			}
		}
		log.Println("Unknown component interaction:", customID)

	case discordgo.InteractionApplicationCommandAutocomplete:
		name := i.ApplicationCommandData().Name
		if handler, ok := AutocompleteHandlers[name]; ok {
			handler(discord, i)
		} else {
			log.Println("Unknown autocomplete interaction for command:", name)
		}
	}
}
