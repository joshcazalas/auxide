package bot

import (
	"strings"

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
	case strings.Contains(m.Content, "!help"):
		s.ChannelMessageSend(m.ChannelID, "")
	case strings.Contains(m.Content, "!bye"):
		s.ChannelMessageSend(m.ChannelID, "Goodbye")
	}
}
