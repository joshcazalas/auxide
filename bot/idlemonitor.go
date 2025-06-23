package bot

import (
	"context"
	"log"
	"time"

	"github.com/bwmarrin/discordgo"
)

var idleCancelFuncs = make(map[string]context.CancelFunc)

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
				if GlobalQueue.IsPlaying(guildID) {
					continue
				}

				last := GlobalQueue.GetLastActivity(guildID)
				if time.Since(last) > 15*time.Minute {
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
					GlobalQueue.Unlock()

					_, _ = discord.ChannelMessageSend(textChannelID, "💤 Left the voice channel after 15 minutes of inactivity.")
					return
				}

				if vc, ok := GlobalQueue.GetVoiceConnection(guildID); ok && vc != nil {
					channel, err := discord.State.Channel(vc.ChannelID)
					if err == nil {
						guild, _ := discord.State.Guild(guildID)
						nonBotCount := 0
						for _, vs := range guild.VoiceStates {
							if vs.ChannelID == channel.ID && vs.UserID != discord.State.User.ID {
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
							GlobalQueue.Unlock()

							_, _ = discord.ChannelMessageSend(textChannelID, "👋 Left the voice channel because no users were present.")
							return
						}
					}
				}
			}
		}
	}()
}

func CancelIdleMonitor(guildID string) {
	GlobalQueue.Lock()
	defer GlobalQueue.Unlock()

	if cancel, ok := idleCancelFuncs[guildID]; ok {
		cancel()
		delete(idleCancelFuncs, guildID)
	}
}
