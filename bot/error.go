package bot

import (
	"fmt"

	"github.com/bwmarrin/discordgo"
)

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
