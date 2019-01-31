package main

import (
	"time"

	"github.com/hellofresh/goengine"
)

// RecipeCreated ...
type RecipeCreated struct {
	occurredOn time.Time
	Name       string
}

// OccurredOn ...
func (e RecipeCreated) OccurredOn() time.Time {
	return e.occurredOn
}

// RecipeRated ...
type RecipeRated struct {
	occurredOn time.Time
	Rating     int
}

// OccurredOn ...
func (e RecipeRated) OccurredOn() time.Time {
	return e.occurredOn
}

// Recipe ...
type Recipe struct {
	*goengine.AggregateRootBased
	Name   string
	Rating int
}

// NewRecipe ...
func NewRecipe(name string) *Recipe {
	recipe := new(Recipe)
	recipe.AggregateRootBased = goengine.NewAggregateRootBased(recipe)
	recipe.RecordThat(&RecipeCreated{time.Now(), name})

	return recipe
}

// NewRecipeFromHistory ...
func NewRecipeFromHistory(id string, streamName goengine.StreamName, repo goengine.AggregateRepository) (*Recipe, error) {
	recipe := new(Recipe)
	recipe.AggregateRootBased = goengine.NewEventSourceBasedWithID(recipe, id)
	err := repo.Reconstitute(id, recipe, streamName)

	return recipe, err
}

// Rate ...
func (r *Recipe) Rate(rate int) {
	r.RecordThat(&RecipeRated{time.Now(), rate})
}

// WhenRecipeCreated ...
func (r *Recipe) WhenRecipeCreated(event *RecipeCreated) {
	r.Name = event.Name
}

// WhenRecipeRated ...
func (r *Recipe) WhenRecipeRated(event *RecipeRated) {
	r.Rating = event.Rating
}
