package bot

import (
	"context"
	"log"
	"time"

	"github.com/bwmarrin/discordgo"
)

func StartIdleMonitor(guildID, textChannelID string, discord *discordgo.Session) {
	CancelIdleMonitor(guildID)

	ctx, cancel := context.WithCancel(context.Background())

	GlobalQueue.Lock()
	GlobalQueue.idleCancelFuncs[guildID] = cancel
	GlobalQueue.Unlock()

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if vc, ok := GlobalQueue.GetVoiceConnection(guildID); ok && vc != nil {
					channelID := vc.ChannelID
					guild, err := discord.State.Guild(guildID)
					if err != nil {
						log.Printf("Failed to get guild state for %s: %v", guildID, err)
						continue
					}

					nonBotCount := 0
					for _, vs := range guild.VoiceStates {
						if vs.ChannelID == channelID && vs.UserID != discord.State.User.ID {
							member, err := discord.State.Member(guildID, vs.UserID)
							if err == nil && !member.User.Bot {
								nonBotCount++
							}
						}
					}

					if nonBotCount == 0 {
						log.Printf("No users in VC in guild %s, leaving.", guildID)

						if err := vc.Disconnect(); err != nil {
							log.Printf("Error disconnecting: %v", err)
						}

						GlobalQueue.SetInVoiceChannel(guildID, false)
						GlobalQueue.SetPlaying(guildID, false)

						GlobalQueue.Lock()
						delete(GlobalQueue.stopChans, guildID)
						delete(GlobalQueue.idleCancelFuncs, guildID)
						GlobalQueue.Unlock()

						embed := &discordgo.MessageEmbed{
							Title:       "👋 Voice Channel Empty",
							Description: "Left the voice channel because no users were present.",
							Color:       0x1DB954,
						}
						_, _ = discord.ChannelMessageSendEmbed(textChannelID, embed)
						return
					}
				}

				if GlobalQueue.IsPlaying(guildID) {
					continue
				}

				last := GlobalQueue.GetLastActivity(guildID)
				if time.Since(last) > 10*time.Minute {
					log.Printf("Idle timeout reached for guild %s", guildID)

					vc, ok := GlobalQueue.GetVoiceConnection(guildID)
					if ok && vc != nil {
						if err := vc.Disconnect(); err != nil {
							log.Printf("Failed to disconnect from voice in guild %s: %v", guildID, err)
						}
					}

					GlobalQueue.SetInVoiceChannel(guildID, false)
					GlobalQueue.SetPlaying(guildID, false)

					GlobalQueue.Lock()
					delete(GlobalQueue.stopChans, guildID)
					delete(GlobalQueue.idleCancelFuncs, guildID)
					GlobalQueue.Unlock()

					embed := &discordgo.MessageEmbed{
						Title:       "💤 Idle Timeout",
						Description: "Left the voice channel after 10 minutes of inactivity.",
						Color:       0x1DB954,
					}
					_, _ = discord.ChannelMessageSendEmbed(textChannelID, embed)
					return
				}
			}
		}
	}()
}

func CancelIdleMonitor(guildID string) {
	GlobalQueue.Lock()
	defer GlobalQueue.Unlock()

	if cancel, ok := GlobalQueue.idleCancelFuncs[guildID]; ok {
		cancel()
		delete(GlobalQueue.idleCancelFuncs, guildID)
	}
}
