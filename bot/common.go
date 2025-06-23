package bot

import (
	"fmt"
	"log"

	"github.com/bwmarrin/discordgo"
)

func CheckNilErr(e error) {
	if e != nil {
		log.Fatalf("FATAL ERROR: %v", e)
	}
}

func GetUserID(i *discordgo.InteractionCreate) string {
	if i.Member != nil {
		return i.Member.User.ID
	}
	if i.User != nil {
		return i.User.ID
	}
	return ""
}

func GetTextChannel(discord *discordgo.Session) (string, error) {
	for _, guild := range discord.State.Guilds {
		channels, err := discord.GuildChannels(guild.ID)
		if err != nil {
			continue
		}
		for _, ch := range channels {
			if ch.Type == discordgo.ChannelTypeGuildText {
				return ch.ID, nil
			}
		}
	}
	return "", fmt.Errorf("no text channel found in any guild")
}
