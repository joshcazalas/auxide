package bot

import "github.com/bwmarrin/discordgo"

type ComponentHandler func(s *discordgo.Session, i *discordgo.InteractionCreate)

var ComponentHandlers = map[string]ComponentHandler{}

func RegisterComponentHandlers() {
	ComponentHandlers["select_video_"] = HandlePlaySelection
}
