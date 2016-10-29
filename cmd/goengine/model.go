package main

import (
	"time"

	"github.com/hellofresh/goengine/eventsourcing"
	"github.com/hellofresh/goengine/eventstore"
)

type RecipeCreated struct {
	ocurredOn time.Time
	Name      string
}

func (e RecipeCreated) OcurredOn() time.Time {
	return e.ocurredOn
}

type RecipeRated struct {
	ocurredOn time.Time
	Rating    int
}

func (e RecipeRated) OcurredOn() time.Time {
	return e.ocurredOn
}

type Recipe struct {
	*eventsourcing.AggregateRootBased
	Name   string
	Rating int
}

func NewRecipe(name string) *Recipe {
	recipe := new(Recipe)
	recipe.AggregateRootBased = eventsourcing.NewAggregateRootBased(recipe)
	recipe.RecordThat(&RecipeCreated{time.Now(), name})

	return recipe
}

func NewRecipeFromHisotry(id string, streamName eventstore.StreamName, repo eventsourcing.AggregateRepository) (*Recipe, error) {
	recipe := new(Recipe)
	recipe.AggregateRootBased = eventsourcing.NewEventSourceBasedWithID(recipe, id)
	err := repo.Reconstitute(id, recipe, streamName)

	return recipe, err
}

func (r *Recipe) Rate(rate int) {
	r.RecordThat(&RecipeRated{time.Now(), rate})
}

func (r *Recipe) WhenRecipeCreated(event *RecipeCreated) {
	r.Name = event.Name
}

func (r *Recipe) WhenRecipeRated(event *RecipeRated) {
	r.Rating = event.Rating
}
