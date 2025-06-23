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

func GetTextChannel(discord *discordgo.Session, guildID string) (string, error) {
	if ch, ok := botTextChannels[guildID]; ok {
		return ch, nil
	}

	return FindOrCreateBotChannel(discord, guildID)
}

func InitializeBotChannels(discord *discordgo.Session) error {
	guilds, err := discord.UserGuilds(100, "", "", false)
	if err != nil {
		return fmt.Errorf("failed to get bot guilds: %w", err)
	}

	for _, g := range guilds {
		channelID, err := FindOrCreateBotChannel(discord, g.ID)
		if err != nil {
			log.Printf("Error initializing bot channel for guild %s: %v", g.ID, err)
			ErrorChan <- GuildError{
				GuildID: g.ID,
				Err:     err,
			}

			msg := "⚠️ I could not create my dedicated channel due to missing permissions. Using a fallback channel instead. Please grant me the 'Manage Channels' permission."
			_, sendErr := discord.ChannelMessageSend(channelID, msg)
			if sendErr != nil {
				log.Printf("Failed to send fallback message in guild %s channel %s: %v", g.ID, channelID, sendErr)
			}
		} else {
			botTextChannels[g.ID] = channelID
		}
	}
	return nil
}

func FindOrCreateBotChannel(discord *discordgo.Session, guildID string) (string, error) {
	channels, err := discord.GuildChannels(guildID)
	if err != nil {
		return "", fmt.Errorf("failed to get channels for guild %s: %w", guildID, err)
	}

	// Check if bot text channel already exists
	for _, ch := range channels {
		if ch.Type == discordgo.ChannelTypeGuildText && ch.Name == BotTextChannelName {
			botTextChannels[guildID] = ch.ID
			return ch.ID, nil
		}
	}

	channel, err := discord.GuildChannelCreate(guildID, BotTextChannelName, discordgo.ChannelTypeGuildText)
	if err != nil {
		log.Printf("Failed to create bot channel for guild %s: %v", guildID, err)

		// Find fallback text channel (e.g. "general" or first text channel)
		for _, ch := range channels {
			if ch.Type == discordgo.ChannelTypeGuildText && ch.Name == "general" {
				botTextChannels[guildID] = ch.ID
				return ch.ID, fmt.Errorf("failed to create bot channel: %w (falling back to general channel)", err)
			}
		}

		// If no "general" found, fallback to first text channel
		for _, ch := range channels {
			if ch.Type == discordgo.ChannelTypeGuildText {
				botTextChannels[guildID] = ch.ID
				return ch.ID, fmt.Errorf("failed to create bot channel: %w (falling back to first text channel)", err)
			}
		}

		return "", fmt.Errorf("failed to create bot channel and no fallback channel found: %w", err)
	}

	botTextChannels[guildID] = channel.ID
	return channel.ID, nil
}
