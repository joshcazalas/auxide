package bot

import "github.com/bwmarrin/discordgo"

type ComponentHandler func(s *discordgo.Session, i *discordgo.InteractionCreate)

var ComponentHandlers = map[string]ComponentHandler{}

func RegisterComponentHandlers() {
	ComponentHandlers["select_video_"] = HandlePlaySelection
}

type AutocompleteHandler func(s *discordgo.Session, i *discordgo.InteractionCreate)

var AutocompleteHandlers = map[string]AutocompleteHandler{}

func RegisterAutocompleteHandler(commandName string, handler AutocompleteHandler) {
	AutocompleteHandlers[commandName] = handler
}

func RegisterAutocompleteHandlers() {
	RegisterAutocompleteHandler("shuffle", HandleShuffleAutocomplete)
}
