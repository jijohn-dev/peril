package main

import (
	"fmt"

	"github.com/jijohn-dev/peril/internal/gamelogic"
	"github.com/jijohn-dev/peril/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
