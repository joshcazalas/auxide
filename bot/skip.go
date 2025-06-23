package bot

import (
	"fmt"
	"log"
	"time"

	"github.com/bwmarrin/discordgo"
)

func HandleSkipCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID
	channelID := i.ChannelID

	if !GlobalQueue.IsInVoiceChannel(guildID) {
		embed := &discordgo.MessageEmbed{
			Title:       "🔇 Not in Voice Channel",
			Description: "I'm not currently in a voice channel.",
			Color:       0x1DB954,
		}
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		})
		return
	}

	if !GlobalQueue.IsPlaying(guildID) {
		embed := &discordgo.MessageEmbed{
			Title:       "⏹️ Nothing Playing",
			Description: "There's no track currently playing to skip.",
			Color:       0x1DB954,
		}
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		})
		return
	}

	next, ok := GlobalQueue.Peek(channelID)
	if !ok {
		embed := &discordgo.MessageEmbed{
			Title:       "📭 Queue Empty",
			Description: "No more songs left in the queue to skip to.",
			Color:       0x1DB954,
		}
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		})
		return
	}

	GlobalQueue.Lock()
	stopChan, found := GlobalQueue.stopChans[guildID]
	GlobalQueue.Unlock()

	_, popped := GlobalQueue.Pop(channelID)
	if !popped {
		log.Printf("Warning: tried to pop current track in skip but queue was empty for channel %s", channelID)
	}

	if found {
		select {
		case stopChan <- true:
			log.Printf("Skip signal sent for guild %s", guildID)
		default:
			log.Printf("Skip channel full or not listening for guild %s", guildID)
		}
	} else {
		log.Printf("No active stop channel for guild %s", guildID)
	}

	GlobalQueue.SetPlaying(guildID, false)

	duration := time.Duration(next.Duration) * time.Second
	embed := &discordgo.MessageEmbed{
		Title:       "⏭️ Skipping to Next Track",
		Description: fmt.Sprintf("[%s](%s)", next.Title, next.WebURL),
		Color:       0x1DB954,
		Fields: []*discordgo.MessageEmbedField{
			{
				Name:   "Requested By",
				Value:  fmt.Sprintf("<@%s>", next.RequestedBy),
				Inline: true,
			},
			{
				Name:   "Duration",
				Value:  fmtDuration(duration),
				Inline: true,
			},
		},
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Try /play to add new songs, or /help for all commands",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})

	go StartPlaybackIfNotActive(discord, guildID, channelID)
}
