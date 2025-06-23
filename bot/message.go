package bot

import (
	"github.com/bwmarrin/discordgo"
)

func Message(s *discordgo.Session, m *discordgo.MessageCreate) {
	if m.Author.ID == s.State.User.ID {
		return
	}

	switch {
	case m.Author.ID == "hbalbin44":
		s.ChannelMessageSend(m.ChannelID, "fuck you")
	case m.Author.ID == "about78kids":
		s.ChannelMessageSend(m.ChannelID, "love you sexy")
	default:
		s.ChannelMessageSend(m.ChannelID, "Use slash commands to trigger actions. Try `/help` to see available commands.")
	}
}
