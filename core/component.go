package core

import (
	"github.com/bwmarrin/discordgo"
	"github.com/joshcazalas/discord-music-bot/cmd"
)

func Component(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionMessageComponent {
		return
	}
	userID := GetUserID(i)
	cmd.HandlePlayComponent(s, i, userID)
}
