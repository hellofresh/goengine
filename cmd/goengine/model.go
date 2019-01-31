package main

import (
	"time"

	"github.com/hellofresh/goengine"
)

type RecipeCreated struct {
	occurredOn time.Time
	Name       string
}

func (e RecipeCreated) OccurredOn() time.Time {
	return e.occurredOn
}

type RecipeRated struct {
	occurredOn time.Time
	Rating     int
}

func (e RecipeRated) OccurredOn() time.Time {
	return e.occurredOn
}

type Recipe struct {
	*goengine.AggregateRootBased
	Name   string
	Rating int
}

func NewRecipe(name string) *Recipe {
	recipe := new(Recipe)
	recipe.AggregateRootBased = goengine.NewAggregateRootBased(recipe)
	recipe.RecordThat(&RecipeCreated{time.Now(), name})

	return recipe
}

func NewRecipeFromHistory(id string, streamName goengine.StreamName, repo goengine.AggregateRepository) (*Recipe, error) {
	recipe := new(Recipe)
	recipe.AggregateRootBased = goengine.NewEventSourceBasedWithID(recipe, id)
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
