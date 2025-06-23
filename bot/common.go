package bot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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

func InitializeBotChannels(discord *discordgo.Session) error {
	guilds, err := discord.UserGuilds(100, "", "", false)
	if err != nil {
		return fmt.Errorf("failed to get bot guilds: %w", err)
	}

	for _, g := range guilds {
		channelID, err := GetOrCreateBotChannel(discord, g.ID)
		if err != nil {
			log.Printf("Error initializing bot channel for guild %s: %v", g.ID, err)
			ErrorChan <- GuildError{
				GuildID: g.ID,
				Err:     err,
			}

			// Send fallback message on the resolved channel if possible
			if channelID != "" {
				msg := "⚠️ I couldn't create my dedicated channel due to missing permissions. Using a fallback channel instead. Please grant me the 'Manage Channels' permission."
				_, sendErr := discord.ChannelMessageSend(channelID, msg)
				if sendErr != nil {
					log.Printf("Failed to send fallback message in guild %s channel %s: %v", g.ID, channelID, sendErr)
				}
			}
		}
	}
	return nil
}

func GetOrCreateBotChannel(discord *discordgo.Session, guildID string) (string, error) {
	channels, err := discord.GuildChannels(guildID)
	if err != nil {
		return "", fmt.Errorf("failed to get channels for guild %s: %w", guildID, err)
	}

	// 1. Look for bot text channel
	for _, ch := range channels {
		if ch.Type == discordgo.ChannelTypeGuildText && ch.Name == BotTextChannelName {
			botTextChannels[guildID] = ch.ID
			return ch.ID, nil
		}
	}

	// 2. Try to find "general" text channel
	for _, ch := range channels {
		if ch.Type == discordgo.ChannelTypeGuildText && ch.Name == "general" {
			botTextChannels[guildID] = ch.ID
			return ch.ID, nil
		}
	}

	// 3. Fallback to first text channel
	for _, ch := range channels {
		if ch.Type == discordgo.ChannelTypeGuildText {
			botTextChannels[guildID] = ch.ID
			return ch.ID, nil
		}
	}

	// 4. No text channels at all? Try to create bot channel
	channel, err := discord.GuildChannelCreate(guildID, BotTextChannelName, discordgo.ChannelTypeGuildText)
	if err != nil {
		// Creation failed, try again to fallback (in case channels appeared meanwhile)
		for _, ch := range channels {
			if ch.Type == discordgo.ChannelTypeGuildText && ch.Name == "general" {
				botTextChannels[guildID] = ch.ID
				return ch.ID, fmt.Errorf("failed to create bot channel: %w (falling back to general)", err)
			}
		}
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

func StartCleanupRoutine(dir string, interval time.Duration, maxFileAge time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			<-ticker.C

			files, err := os.ReadDir(dir)
			if err != nil {
				log.Printf("Cleanup: failed to read dir %s: %v", dir, err)
				continue
			}

			now := time.Now()
			for _, file := range files {
				path := filepath.Join(dir, file.Name())
				info, err := file.Info()
				if err != nil {
					log.Printf("Cleanup: failed to get info for %s: %v", path, err)
					continue
				}

				// Remove files older than maxFileAge
				if now.Sub(info.ModTime()) > maxFileAge {
					err := os.Remove(path)
					if err != nil {
						log.Printf("Cleanup: failed to remove file %s: %v", path, err)
					} else {
						log.Printf("Cleanup: removed old file %s", path)
					}
				}
			}
		}
	}()
}
