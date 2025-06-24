package bot

import (
	"fmt"
	"time"

	"github.com/bwmarrin/discordgo"
)

func SendNowPlayingEmbed(s *discordgo.Session, channelID string, video VideoInfo) {
	duration := time.Duration(video.Duration) * time.Second

	embed := &discordgo.MessageEmbed{
		Title:       "🎶 Now Playing",
		Description: fmt.Sprintf("[%s](%s)", video.Title, video.WebURL),
		Color:       0x1DB954,
		Fields: []*discordgo.MessageEmbedField{
			{
				Name:   "Requested By",
				Value:  fmt.Sprintf("<@%s>", video.RequestedBy),
				Inline: true,
			},
			{
				Name:   "Duration",
				Value:  fmtDuration(duration),
				Inline: true,
			},
		},
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Try /shuffle, /skip, /stop & more. Use /help to see all commands",
		},
	}

	s.ChannelMessageSendEmbed(channelID, embed)
}

func fmtDuration(d time.Duration) string {
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%d:%02d", minutes, seconds)
}
